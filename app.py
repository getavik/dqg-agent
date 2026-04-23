import streamlit as st
import pandas as pd
import io
import os
import streamlit.components.v1 as components
import plotly.graph_objects as go
from fpdf import FPDF
from src.profiler import generate_profile
from src.governance import detect_pii
from src.llm_engine import analyze_intent, generate_remediation, generate_business_impact
from src.validator import validate_data
from src.reporter import generate_pdf_report

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
    page_title="Governance Bridge | Smart Data Agent",
    page_icon="🛡️"
)

# --- MATERIAL DESIGN 3 STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Roboto', sans-serif;
    }

    /* Top Navigation/Header - Softened gradient */
    .main-header {
        background: linear-gradient(135deg, #6750A4 0%, #4A3A73 100%);
        color: #FFFFFF;
        padding: 1.5rem 2rem;
        border-radius: 0 0 16px 16px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
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
        <h1 style='margin:0; font-size: 2rem;'>🛡️ Governance Bridge</h1>
        <p style='margin:0; opacity: 0.9;'>Autonomous Data Quality & Governance Agent</p>
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

# --- APP LOGIC ---
if not uploaded_file or not intent:
    st.info("👋 Welcome! Please upload a dataset and define your business intent in the sidebar to begin.")
    st.stop()

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
if 'health_score_after' in st.session_state:
    score_before = st.session_state.get('health_score_before', 'N/A')
    score_after = st.session_state.get('health_score_after', 'N/A')
    c4.metric("Data Health", f"{score_after:.1f}%", delta=f"{score_after - score_before:.1f}% improvement")
elif 'health_score_before' in st.session_state:
    score = st.session_state['health_score_before']
    c4.metric("Data Health (Before)", f"{score:.1f}%")
elif 'validation_results' in st.session_state:
    score = st.session_state['validation_results']["statistics"]["success_percent"]
    c4.metric("Data Health", f"{score:.1f}%")
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
    col_btn, col_info = st.columns([1, 3])
    
    with col_btn:
        if st.button("🚀 Run Profiling", use_container_width=True):
            with st.spinner("Analyzing data distribution..."):
                profile_results = generate_profile(df)
                st.session_state['profile_summary'] = profile_results["summary"]
                st.session_state['report_html'] = profile_results["report_html"]
                st.toast("Profiling Complete!", icon="✅")

    if 'report_html' in st.session_state:
        st.markdown("### 📈 Data Distribution Report")
        components.html(st.session_state['report_html'], height=700, scrolling=True)
    else:
        st.info("Start by profiling the data to understand its structure and quality issues.")

# --- TAB 2: GOVERNANCE ---
with tab2:
    st.header("Stage 2: PII Detection & Rule Synthesis")
    
    if 'profile_summary' not in st.session_state:
        st.warning("⚠️ Please complete Stage 1 (Profiling) first.")
    else:
        if st.button("🔍 Synthesize Governance Rules", use_container_width=True):
            with st.spinner("Analyzing Sensitive Data & Mapping DAMA Standards..."):
                pii_results = detect_pii(df, api_key=st.session_state.get('gemini_key'))
                st.session_state['pii_results'] = pii_results

                rules_output = analyze_intent(
                    intent, 
                    st.session_state['profile_summary'], 
                    pii_results,
                    api_key=st.session_state.get('gemini_key')
                )
                st.session_state['rules'] = rules_output["rules"]
                st.toast("Rules Generated!", icon="🛡️")
        if 'rules' in st.session_state:
            col_pii, col_rules = st.columns([1, 2])
            
            with col_pii:
                st.subheader("🕵️ PII Findings")
                st.json(st.session_state.get('pii_results', {}))
            
            with col_rules:
                st.subheader("📝 Synthesized Rules")
                st.dataframe(st.session_state['rules'], use_container_width=True)

# --- TAB 3: EXECUTION ---
with tab3:
    st.header("Stage 3: Validation & Auto-Remediation")
    
    if 'rules' not in st.session_state:
        st.warning("⚠️ Please complete Stage 2 (Governance) first.")
    else:
        if st.button("⚖️ Run Validation", use_container_width=True):
            with st.spinner("Executing Data Quality Gates..."):
                validation_results = validate_data(df, st.session_state['rules'])
                st.session_state['validation_results'] = validation_results
                st.toast("Validation Complete!", icon="📊")

        if 'validation_results' in st.session_state:
            vr = st.session_state['validation_results']
            
            if vr["failures"]:
                st.error(f"🚨 Found {len(vr['failures'])} Failed Expectations")
                st.dataframe(vr["failures"], use_container_width=True)
                
                # Remediation Section
                st.divider()
                st.subheader("🛠️ Remediation Plan")
                plan = generate_remediation(vr["failures"])
                
                for item in plan["remediations"]:
                    with st.expander(f"Fix for {item['issue']}"):
                        st.code(item['python_fix'], language='python')
                
                if st.button("🪄 Apply Auto-Remediation", type="primary"):
                    from src.remediator import apply_remediation
                    st.session_state['original_df_for_report'] = df.copy()
                    fixed_df = apply_remediation(df, vr["failures"])
                    st.session_state['df'] = fixed_df
                    st.session_state['data_remediated'] = True
                    st.session_state['health_score_before'] = vr['statistics']['success_percent']
                    st.session_state['report_snapshot'] = vr
                    
                    # Cleanup for refresh
                    if 'ai_impact_text' in st.session_state: del st.session_state['ai_impact_text']
                    st.rerun()
            else:
                st.success("✨ All validations passed! Data is compliant.")

    if st.session_state.get('data_remediated'):
        st.success("✅ Data remediated and updated in memory.")
        st.subheader("📦 Export Results")
        
        c_csv, c_xlsx, c_pdf = st.columns(3)
        
        with c_csv:
            csv_data = st.session_state['df'].to_csv(index=False).encode('utf-8')
            st.download_button("Download CSV", csv_data, "remediated_data.csv", "text/csv", use_container_width=True)
            
        with c_xlsx:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                st.session_state['original_df_for_report'].to_excel(writer, sheet_name='Original Data', index=False)
                st.session_state['df'].to_excel(writer, sheet_name='Remediated Data', index=False)
            st.download_button("Download Excel", buffer.getvalue(), "remediated_data.xlsx", use_container_width=True)
            
        with c_pdf:
            # Generate impact for report
            imp = generate_business_impact(intent, st.session_state['rules'], 
                                         st.session_state.get('report_snapshot', {}).get("failures", []), 
                                         len(st.session_state['df']))
            pdf_data = generate_pdf_report(
                df_original=st.session_state['df'], # Simplified for demo
                df_fixed=st.session_state['df'],
                rules=st.session_state['rules'],
                validation_results=st.session_state.get('report_snapshot', {}),
                intent=intent,
                impact_text=imp
            )
            st.download_button("Download Audit Report", pdf_data, "audit_report.pdf", use_container_width=True)

# --- TAB 4: INTELLIGENCE ---
with tab4:
    st.header("🤖 AI Strategic Analysis")
    st.caption("Deep risk analysis and ROI forecasting powered by Google Gemini.")
    
    if not st.session_state.get('data_remediated'):
        st.info("Apply auto-remediation in Tab 3 to unlock AI Strategic Insights.")
    else:
        if st.button("🚀 Generate Strategic Intelligence", type="primary"):
            if not st.session_state.get('gemini_key'):
                st.error("Missing Gemini API Key in sidebar.")
            else:
                try:
                    with st.status("AI Agent Analyzing...", expanded=True) as status:
                        st.write("📊 Contextualizing Governance Metadata...")
                        # Context Prep
                        df_ctx = st.session_state['df']
                        rev_col = next((c for c in df_ctx.columns if any(x in c.lower() for x in ['amount', 'revenue'])), None)
                        total_rev = f"{pd.to_numeric(df_ctx[rev_col], errors='coerce').sum():,.2f}" if rev_col else "N/A"
                        ctx_str = f"Columns: {list(df_ctx.columns)}\nFinancial Value: {total_rev}"
                        
                        st.write("🧠 Synthesizing Strategic Impact...")
                        ai_text = generate_business_impact(
                            intent=intent,
                            rules=st.session_state['rules'],
                            failures=st.session_state.get('report_snapshot', {}).get("failures", []),
                            total_rows=len(df_ctx),
                            api_key=st.session_state['gemini_key'],
                            df_summary=ctx_str
                        )
                        st.session_state['ai_impact_text'] = ai_text
                        status.update(label="✅ Analysis Ready", state="complete")
                except Exception as e:
                    st.error(f"AI Error: {e}")

        if 'ai_impact_text' in st.session_state:
            st.markdown("### 📊 Data Quality Improvement")
            
            # Get the number of failures before and after remediation
            before_failures = st.session_state.get('report_snapshot', {}).get("failures", [])
            
    # Rerun validation on the cleaned data to get the number of failures after remediation
    validation_results_after = validate_data(st.session_state['df'], st.session_state['rules'])
    st.session_state['health_score_after'] = validation_results_after['statistics']['success_percent']
    after_failures = validation_results_after.get("failures", [])
    
            # Create and display the chart
            fig = create_dq_improvement_chart(before_failures, after_failures)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown(f"### {st.session_state['ai_impact_text'].get('title', 'Strategic Brief')}")
            st.write(st.session_state['ai_impact_text'].get('summary', ''))
            
            for insight in st.session_state['ai_impact_text'].get('insights', []):
                with st.container():
                    st.subheader(insight.get('title'))
                    st.write(insight.get('text'))
            
            # Premium PDF Download
            pdf_premium = generate_pdf_report(
                df_original=st.session_state['df'],
                df_fixed=st.session_state['df'],
                rules=st.session_state['rules'],
                validation_results=st.session_state.get('report_snapshot', {}),
                intent=intent,
                impact_text=st.session_state['ai_impact_text'].get('summary', '')
            )
            st.download_button("📥 Download Premium AI Report", pdf_premium, "strategic_audit.pdf", type="primary")
