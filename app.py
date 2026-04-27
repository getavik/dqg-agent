import streamlit as st
import pandas as pd
import io
import os
import json
import streamlit.components.v1 as components
import plotly.graph_objects as go
from src.profiler import generate_profile
from src.governance import detect_pii
from src.llm_engine import analyze_intent, generate_remediation, generate_business_impact
from src.dq_utils import validate_and_tag_data, remediate_data
from src.reporter import generate_pdf_report, format_profiling_summary_for_excel, generate_profiling_pdf_from_html

def create_dq_improvement_chart(before_failures, after_failures):
    """Creates a bar chart showing the reduction in data quality issues."""
    
    before_count = len(before_failures)
    after_count = len(after_failures)
    
    fig = go.Figure(data=[
        go.Bar(name='Before Remediation', x=['Data Quality Issues'], y=[before_count]),
        go.Bar(name='After Remediation', x=['Data Quality Issues'], y=[after_count])
    ])
    
    fig.update_layout(
        title='Data Quality Improvement',
        yaxis_title='Number of Failed Expectations',
        barmode='group'
    )
    return fig


# --- CONFIGURATION ---
st.set_page_config(
    layout="wide", 
    page_title="Smart Data Analyst",
    page_icon="🧠"
)

# --- INITIALIZE SESSION STATE ---
if "total_input_tokens" not in st.session_state:
    st.session_state.total_input_tokens = 0
if "total_output_tokens" not in st.session_state:
    st.session_state.total_output_tokens = 0
if "estimated_cost" not in st.session_state:
    st.session_state.estimated_cost = 0.0

# --- MODEL PRICING ---
MODEL_PRICING = {
    "models/gemini-2.5-pro": {"input": 0.5 / 1_000_000, "output": 1.5 / 1_000_000},
    "models/gemini-pro-latest": {"input": 0.5 / 1_000_000, "output": 1.5 / 1_000_000},
    # Add other models as needed
}

# --- MATERIAL DESIGN 3 STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=EB+Garamond:wght@500&family=Inter:wght@200&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        font-weight: 200;
    }

    h1, h2, h3, h4, h5, h6, .main-header, .stTabs [data-baseweb="tab"] {
        font-family: 'EB Garamond', serif;
        font-weight: 500;
    }

    /* Top Navigation/Header - Softened gradient */
    .main-header {
        background: linear-gradient(135deg, #4A3A73 0%, #3C2D5E 100%);
        color: #FFFFFF;
        padding: 2rem 2.5rem;
        border-radius: 0 0 18px 18px;
        margin-bottom: 2rem;
        box-shadow: 0 6px 20px rgba(0,0,0,0.18);
    }

    /* Tabs Styling - Let Streamlit handle base colors to preserve contrast */
    .stTabs [data-baseweb="tab"] {
        border-radius: 12px 12px 0 0;
        padding: 0 24px;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        border-bottom: 3px solid #6750A4 !important;
    }

    /* Cards & Containers - Softer borders */
    .m3-card {
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid rgba(103, 80, 164, 0.2);
        margin-bottom: 1rem;
        background-color: transparent;
    }

    /* Buttons - Tonal Primary */
    .stButton>button {
        background-color: #F0EBF8 !important;
        color: #6750A4 !important;
        border-radius: 100px !important;
        padding: 0.5rem 1.5rem !important;
        font-weight: 600 !important;
        border: 1px solid #D0C3E3 !important;
        transition: all 0.2s ease;
    }

    .stButton>button:hover {
        background-color: #6750A4 !important;
        color: #FFFFFF !important;
        box-shadow: 0 4px 8px rgba(103, 80, 164, 0.25) !important;
    }

    /* Metrics - Better readability */
    [data-testid="stMetricValue"] {
        color: #4A3A73;
        font-weight: 700;
        font-size: 2.2rem;
    }
</style>
""", unsafe_allow_html=True)

# --- HEADER ---
st.markdown("""
    <div class="main-header">
        <h1 style='font-family: "EB Garamond", serif; font-weight: 500; margin:0; font-size: 2.5rem;'>🧠 Smart Data Analyst</h1>
        <p style='font-family: "Inter", sans-serif; font-weight: 200; margin:0; opacity: 0.9;'>Autonomous Data Quality & Governance Agent</p>
    </div>
""", unsafe_allow_html=True)

# --- SIDEBAR: SETUP ---
with st.sidebar:
    st.header("⚙️ Configuration")
    uploaded_file = st.file_uploader("Source Dataset (CSV)", type=["csv"])
    intent = st.text_area("Business Intent", placeholder="e.g., Optimize Marketing ROI for Q4", help="The primary goal for this data.")
    criticality = st.select_slider("Data Criticality", options=["Low", "Medium", "High"], value="Medium")
    
    st.divider()
    st.header("🔑 Intelligence")
    gemini_key = st.text_input("Gemini API Key", type="password", help="Enables Strategic AI Insights.")
    
    if gemini_key:
        st.success("✨ AI Engine Active")
        st.session_state['gemini_key'] = gemini_key
    else:
        st.info("💡 Add Gemini Key for AI features.")
        st.session_state['gemini_key'] = None

    st.divider()
    st.subheader("Session API Usage")
    col1, col2 = st.columns(2)
    col1.metric("Input Tokens", st.session_state.total_input_tokens)
    col2.metric("Output Tokens", st.session_state.total_output_tokens)
    st.metric("Estimated Cost", f"${st.session_state.estimated_cost:.5f}")

# --- APP LOGIC ---
if not uploaded_file or not intent:
    st.info("👋 Welcome! Please upload a dataset and define your business intent in the sidebar to begin.")
    st.stop()

# Check for new file upload and reset state if necessary
if 'current_file' not in st.session_state or st.session_state['current_file'] != uploaded_file.name:
    for key in list(st.session_state.keys()):
        if key not in ['gemini_key', 'total_input_tokens', 'total_output_tokens', 'estimated_cost']:
            del st.session_state[key]
    st.session_state['current_file'] = uploaded_file.name

# Load Data
if 'df' not in st.session_state:
    st.session_state['df'] = pd.read_csv(uploaded_file)
    st.session_state['original_df_shape'] = st.session_state['df'].shape

df = st.session_state['df']

# Quick Stats Ribbon
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{df.shape[0]:,}")
c2.metric("Columns", f"{df.shape[1]:,}")
c3.metric("Criticality", criticality)

score_after = st.session_state.get('health_score_after')
score_before = st.session_state.get('health_score_before')

if score_after is not None:
    if score_before is not None and st.session_state.get('data_remediated'):
        delta = score_after - score_before
        c4.metric("Data Health", f"{score_after:.1f}%", delta=f"{delta:.1f}% improvement")
    else:
        c4.metric("Data Health", f"{score_after:.1f}%")
elif score_before is not None:
    c4.metric("Data Health (Initial)", f"{score_before:.1f}%")
else:
    c4.metric("Data Health", "Pending")

st.divider()

# --- MAIN WORKFLOW TABS ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Profiling", 
    "🛡️ Governance", 
    "⚡ Execution", 
    "🤖 Strategic Intelligence"
])

# --- TAB 1: PROFILING ---
with tab1:
    st.header("Stage 1: Technical Metadata Discovery")
    
    # Use remediated data if available
    data_for_profiling = st.session_state.get('remediated_df', df)
    
    if st.button("🚀 Run Profiling", key="run_profiling"):
        with st.spinner("Analyzing data distribution..."):
            profile_results = generate_profile(data_for_profiling)
            st.session_state['profile_results'] = profile_results
            st.session_state['profile_summary'] = profile_results["summary"]
            st.session_state['report_html'] = profile_results["report_html"]
            
            # Calculate Initial Health Score based on completeness
            cols_stats = profile_results["summary"]["columns"]
            if cols_stats:
                avg_missing = sum(s["p_missing"] for s in cols_stats.values()) / len(cols_stats)
                st.session_state['health_score_before'] = (1 - avg_missing) * 100
            
            st.toast("Profiling Complete!", icon="✅")

    if 'report_html' in st.session_state:
        st.markdown("### 📈 Data Distribution Report")
        
        # Download button for PDF report
        pdf_data = generate_profiling_pdf_from_html(st.session_state['report_html'])
        st.download_button(
            label="Download Profiling Report (PDF)",
            data=pdf_data,
            file_name="profiling_report.pdf",
            mime="application/pdf",
            key="download_profiling"
        )
        
        components.html(st.session_state['report_html'], height=700, scrolling=True)
    else:
        st.info("Start by profiling the data to understand its structure and quality issues.")

# --- TAB 2: GOVERNANCE ---
with tab2:
    st.header("Stage 2: PII Detection & Rule Synthesis")
    
    if 'profile_summary' not in st.session_state:
        st.warning("⚠️ Please complete Stage 1 (Profiling) first.")
    else:
        if st.button("🔍 Synthesize Governance Rules", key="synthesize_rules"):
            if 'rules' in st.session_state: del st.session_state['rules']
            if 'pii_results' in st.session_state: del st.session_state['pii_results']
            
            with st.spinner("Analyzing Sensitive Data & Mapping DAMA Standards..."):
                pii_results = detect_pii(df, api_key=st.session_state.get('gemini_key'))
                st.session_state['pii_results'] = pii_results

                rules_output, usage = analyze_intent(
                    intent, 
                    st.session_state['profile_summary'], 
                    pii_results,
                    api_key=st.session_state.get('gemini_key')
                )
                if usage:
                    st.session_state.total_input_tokens += usage['prompt_token_count']
                    st.session_state.total_output_tokens += usage['candidates_token_count']
                    pricing = MODEL_PRICING.get(usage['model_name'], {"input": 0, "output": 0})
                    st.session_state.estimated_cost += (usage['prompt_token_count'] * pricing["input"]) + (usage['candidates_token_count'] * pricing["output"])

                st.session_state['rules'] = rules_output["rules"]
                st.toast("Rules Generated!", icon="🛡️")
        if 'rules' in st.session_state:
            st.subheader("🕵️ PII Findings")
            
            pii_json_data = json.dumps(st.session_state.get('pii_results', {}), indent=4)
            st.download_button(
                label="Download PII Findings as JSON",
                data=pii_json_data,
                file_name="pii_findings.json",
                mime="application/json",
                key="download_pii"
            )

            st.subheader("📝 Synthesized Rules")
            st.dataframe(pd.DataFrame(st.session_state['rules']))

# --- TAB 3: EXECUTION ---
with tab3:
    st.header("Stage 3: Validation & Auto-Remediation")
    
    if 'rules' not in st.session_state:
        st.warning("⚠️ Please complete Stage 2 (Governance) first.")
    else:
        if st.button("⚖️ Run Validation & Remediation", key="run_validation"):
            with st.spinner("Executing Data Quality Gates and Applying Fixes..."):
                tagged_df = validate_and_tag_data(df, st.session_state['rules'])
                remediated_df = remediate_data(tagged_df, st.session_state['rules'], st.session_state.get('pii_results', {}))
                st.session_state['remediated_df'] = remediated_df
                st.toast("Validation and Remediation Complete!", icon="✅")

        if 'remediated_df' in st.session_state:
            st.subheader("Data Remediation Summary")
            remediated_df = st.session_state['remediated_df']
            
            st.dataframe(remediated_df)

            st.subheader("📦 Export Results")
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                st.session_state['df'].to_excel(writer, sheet_name='Initial Data', index=False)
                remediated_df.to_excel(writer, sheet_name='Remediated Data', index=False)
                if 'rules' in st.session_state:
                    pd.DataFrame(st.session_state['rules']).to_excel(writer, sheet_name='Synthesized Rules', index=False)

            st.download_button(
                label="Download Remediation Report (Excel)",
                data=buffer.getvalue(),
                file_name="dq_remediation_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_remediation"
            )

# --- TAB 4: INTELLIGENCE ---
with tab4:
    st.header("🤖 AI Strategic Analysis")
    st.caption("Deep risk analysis and ROI forecasting powered by Google Gemini.")
    
    if 'remediated_df' not in st.session_state:
        st.info("Complete Stage 3 (Validation & Remediation) to unlock AI Strategic Insights.")
    else:
        if st.button("🚀 Generate Strategic Intelligence", key="run_intelligence"):
            if not st.session_state.get('gemini_key'):
                st.error("Missing Gemini API Key in sidebar.")
            else:
                try:
                    with st.spinner("AI Agent Analyzing..."):
                        remediated_df = st.session_state['remediated_df']
                        df_ctx = remediated_df.drop(columns=['dq_status'], errors='ignore')
                        
                        # Detect failures for the AI prompt
                        # We'll use a simple heuristic: any row not 'good' is a failure
                        failures_count = len(remediated_df[remediated_df['dq_status'] != 'good'])
                        
                        # Create a mock failures list for the AI engine
                        mock_failures = []
                        # (We could be more detailed here if we parsed dq_status)
                        mock_failures.append({"column": "Multiple", "expectation": "Various", "unexpected_count": failures_count})

                        rev_col = next((c for c in df_ctx.columns if any(x in c.lower() for x in ['amount', 'revenue'])), None)
                        total_rev = f"{pd.to_numeric(df_ctx[rev_col], errors='coerce').sum():,.2f}" if rev_col else "N/A"
                        ctx_str = f"Columns: {list(df_ctx.columns)}\nFinancial Value: {total_rev}"
                        
                        ai_text, usage = generate_business_impact(
                            intent=intent,
                            rules=st.session_state['rules'],
                            failures=mock_failures,
                            total_rows=len(df_ctx),
                            api_key=st.session_state['gemini_key'],
                            df_summary=ctx_str
                        )
                        if usage:
                            st.session_state.total_input_tokens += usage['prompt_token_count']
                            st.session_state.total_output_tokens += usage['candidates_token_count']
                            pricing = MODEL_PRICING.get(usage['model_name'], {"input": 0, "output": 0})
                            st.session_state.estimated_cost += (usage['prompt_token_count'] * pricing["input"]) + (usage['candidates_token_count'] * pricing["output"])

                        st.session_state['ai_impact_text'] = ai_text
                except Exception as e:
                    st.error(f"AI Error: {e}")

        if 'ai_impact_text' in st.session_state:
            st.markdown("### 📊 Data Quality Insights")
            
            remediated_df = st.session_state['remediated_df']
            good_count = len(remediated_df[remediated_df['dq_status'] == 'good'])
            fixed_count = len(remediated_df[remediated_df['dq_status'].str.contains('fixed', na=False)])
            bad_count = len(remediated_df[remediated_df['dq_status'].str.contains('bad', na=False)])
            
            # Simple bar chart for record status
            fig = go.Figure(data=[
                go.Bar(name='Records', x=['Good', 'Fixed', 'Bad (Manual)'], y=[good_count, fixed_count, bad_count])
            ])
            st.plotly_chart(fig)

            st.markdown(f"### {st.session_state['ai_impact_text'].get('title', 'Strategic Brief')}")
            st.write(st.session_state['ai_impact_text'].get('summary', ''))
            
            for insight in st.session_state['ai_impact_text'].get('insights', []):
                st.subheader(insight.get('title'))
                st.write(insight.get('text'))
            
            # Use original df and remediated df for the audit report
            pdf_premium = generate_pdf_report(
                df_original=st.session_state['df'],
                df_fixed=st.session_state['remediated_df'],
                rules=st.session_state['rules'],
                validation_results={"failures": [], "statistics": {"success_percent": (good_count/len(remediated_df)*100)}},
                intent=intent,
                impact_text=st.session_state['ai_impact_text'].get('summary', '')
            )
            st.download_button("📥 Download Premium AI Report", pdf_premium, "strategic_audit.pdf", key="download_premium")


