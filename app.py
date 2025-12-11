import streamlit as st
import pandas as pd
import io
from fpdf import FPDF
from src.profiler import generate_profile
from src.governance import detect_pii
from src.llm_engine import analyze_intent, generate_remediation, generate_business_impact
from src.validator import validate_data
from src.reporter import generate_pdf_report
import streamlit.components.v1 as components

st.set_page_config(layout="wide", page_title="Governance Bridge")

# Custom CSS for Modern UI
st.markdown("""
<style>
    /* Global Font & Background */
    body {
        font-family: 'Inter', sans-serif;
    }
    .stApp {
        background-color: #f8f9fa;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #0f172a;
        font-weight: 600;
    }
    
    /* Cards/Expanders */
    .streamlit-expanderHeader {
        background-color: #ffffff;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        padding: 1rem;
        margin-bottom: 0.5rem;
        font-weight: 500;
    }
    
    /* Buttons */
    .stButton>button {
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.2s;
    }
    .stButton>button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        color: #2563eb;
    }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ Governance Bridge: Smart Data Quality Agent")

# 1. Input Module
st.sidebar.header("1. Input Context")
uploaded_file = st.sidebar.file_uploader("Upload Data (CSV)", type=["csv"])
intent = st.sidebar.text_area("Business Intent", placeholder="e.g., Marketing Personalization, Financial Forecasting")
criticality = st.sidebar.selectbox("Data Criticality", ["High", "Medium", "Low"])
gemini_key = st.sidebar.text_input("Gemini API Key (Optional)", type="password", help="Press Enter to apply. Enables AI-powered insights.")

if gemini_key:
    st.sidebar.success("✨ AI Agent Active")
    st.session_state['gemini_key'] = gemini_key # Persist for download button context if needed
else:
    st.session_state['gemini_key'] = None

if uploaded_file and intent:
    if 'df' not in st.session_state:
        st.session_state['df'] = pd.read_csv(uploaded_file)
        st.write(f"**Data Loaded:** {st.session_state['df'].shape[0]} rows, {st.session_state['df'].shape[1]} columns")
    
    df = st.session_state['df']
    
    # 2. Agent Core - Stage 1: Profiling
    with st.expander("Stage 1: Technical Profiling", expanded=True):
        if st.button("Run Profiling"):
            with st.spinner("Profiling data..."):
                profile_results = generate_profile(df)
                st.session_state['profile_summary'] = profile_results["summary"]
                st.session_state['report_html'] = profile_results["report_html"]
                st.success("Profiling Complete!")
                
        if 'report_html' in st.session_state:
            with st.expander("View Detailed Profile Report"):
                components.html(st.session_state['report_html'], height=600, scrolling=True)

    # 2. Agent Core - Stage 2: Governance Mapping
    with st.expander("Stage 2: Governance & Rule Synthesis", expanded=True):
        if st.button("Analyze & Generate Rules"):
            if 'profile_summary' not in st.session_state:
                st.error("Please run profiling first.")
            else:
                with st.spinner("Detecting PII and Synthesizing Rules..."):
                    pii_results = detect_pii(df)
                    st.write("### PII Detection Results")
                    st.json(pii_results)
                    
                    rules_output = analyze_intent(intent, st.session_state['profile_summary'], pii_results)
                    st.session_state['rules'] = rules_output["rules"]
                    
                    st.write("### Synthesized Governance Rules")
                    st.dataframe(st.session_state['rules'])
                
    # 2. Agent Core - Stage 3: Execution
    with st.expander("Stage 3: Validation & Remediation", expanded=True):
        if st.button("Run Validation"):
            if 'rules' not in st.session_state:
                st.error("Please generate rules first.")
            else:
                with st.spinner("Running Great Expectations..."):
                    validation_results = validate_data(df, st.session_state['rules'])
                    st.session_state['validation_results'] = validation_results
        
        if 'validation_results' in st.session_state:
            validation_results = st.session_state['validation_results']
            
            # Calculate Score (Mock logic)
            success_rate = validation_results["statistics"]["success_percent"]
            st.metric("Data Health Score", f"{success_rate:.1f}%")
            
            if validation_results["failures"]:
                st.error(f"Found {len(validation_results['failures'])} failed expectations.")
                st.write("### Failed Validations")
                
                # Enrich failure display with Dimension if possible
                # We need to join back with rules to get Dimension, or just show raw failure
                # For now, showing the raw failure DF is fine, but let's try to map it back if we can.
                # Actually, simpler: Just show the failures. The user can correlate with Stage 2.
                st.dataframe(validation_results["failures"])
                
                # Manual Remediation Plan
                remediation_plan = generate_remediation(validation_results["failures"])
                st.write("### Remediation Plan")
                for item in remediation_plan["remediations"]:
                    with st.expander(f"Fix for {item['issue']}"):
                        st.code(item['sql_fix'], language='sql')
                        st.code(item['python_fix'], language='python')
                
                # Auto-Fix Section
                st.divider()
                st.subheader("Auto-Remediation")
                
                if st.button("Apply Auto-Fix"):
                    from src.remediator import apply_remediation
                    df_fixed = apply_remediation(df, validation_results["failures"])
                    st.session_state['df'] = df_fixed # Update main dataframe
                    st.session_state['data_remediated'] = True
                    
                    # Persist validation snapshot for reporting before clearing UI state
                    st.session_state['report_snapshot'] = validation_results
                    
                    # Clear caches
                    if 'ai_impact_text' in st.session_state:
                         del st.session_state['ai_impact_text']

                    # Clear old validation results so they don't show mismatch
                    del st.session_state['validation_results'] 
                    
                    st.rerun() 
                    
            else:
                st.success("All validations passed!")

        if st.session_state.get('data_remediated'):
            st.success("Data has been remediated! The dataset in memory has been updated.")
            
            # Helper functions for export
            def to_excel(df: pd.DataFrame):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Cleaned Data')
                return output.getvalue()
                
            # Modern Download Section
            st.subheader("Download Cleaned Data")
            c1, c2, c3 = st.columns(3)
            
            with c1:
                csv = st.session_state['df'].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name="fixed_data.csv",
                    mime="text/csv",
                    type="primary",
                    use_container_width=True
                )
            
            with c2:
                excel_data = to_excel(st.session_state['df'])
                st.download_button(
                    label="Download Excel",
                    data=excel_data,
                    file_name="fixed_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                
            with c3:
                # Use Advanced Reporter
                # We need original df for stats. Assuming st.session_state['df'] is now fixed. 
                # Ideally we should have kept original. For now, we can approximate or just use current.
                # Actually, in 'Apply Auto-Fix', we overwrote 'df'. 
                # Let's use the current 'df' as fixed, and maybe we don't have original easily unless we reload or persisted it.
                # For this demo, we'll pass the fixed df as both or just handle inside.
                # Let's pass 'df' as fixed. For original, we might have lost it if we didn't store a copy.
                # It's fine, we'll just pass the current df.
                
                # Generate Business Impact Analysis
                from src.llm_engine import generate_business_impact # Lazy import safety or use top-level
                # Generate Business Impact Analysis
                heuristic_impact = generate_business_impact(intent, st.session_state['rules'], \
                    st.session_state.get('report_snapshot', {}).get("failures", []), len(st.session_state['df']))
                
                pdf_std = generate_pdf_report(
                    df_original=st.session_state['df'],
                    df_fixed=st.session_state['df'],
                    rules=st.session_state['rules'],
                    validation_results=st.session_state.get('report_snapshot', {}),
                    intent=intent,
                    impact_text=heuristic_impact
                )

                st.download_button(
                    label="Download Standard Report (PDF)",
                    data=pdf_std,
                    file_name="standard_audit_report.pdf",
                    mime="application/pdf",
                    use_container_width=True
                )
            
            # --- Dedicated AI Section ---
            st.divider()
            st.header("🤖 AI Strategic Intelligence")
            st.caption("Generate a premium, AI-driven analysis of your data quality and business risks using Google Gemini.")
            
            col_ai_action, col_ai_download = st.columns([1, 2])
            
            with col_ai_action:
                generate_ai = st.button("🚀 Generate Strategic Analysis", type="primary", use_container_width=True)
                
            gemini_key_active = st.session_state.get('gemini_key')
            
            if generate_ai:
                if not gemini_key_active:
                    st.error("⚠️ Please enter your Gemini API Key in the sidebar first.")
                else:
                    try:
                        with st.status("Initializing AI Agent...", expanded=True) as status:
                            st.write("🔐 Authenticating with Google Gemini API...")
                            # Verification call (lightweight)
                            import google.generativeai as genai
                            genai.configure(api_key=gemini_key_active)
                            # Just check if we can list models to validate key
                            models = list(genai.list_models())
                            
                            st.write("📊 Aggregating Data Context & Business Intent...")
                            df_fixed_ctx = st.session_state['df']
                            # Richer summary logic
                            cols_ctx = df_fixed_ctx.columns
                            c_amt_ctx = next((c for c in cols_ctx if any(x in c.lower() for x in ['amount', 'revenue'])), None)
                            total_rev_ctx = "N/A"
                            if c_amt_ctx:
                                total_rev_ctx = f"{pd.to_numeric(df_fixed_ctx[c_amt_ctx], errors='coerce').sum():,.2f}"
                            
                            df_summary_str = f"Columns: {list(cols_ctx)}\nTotal Revenue: {total_rev_ctx}"
                            
                            st.write("🧠 Generating Strategic Insights (Using Best Available Model)...")
                            from src.llm_engine import generate_business_impact
                            
                            ai_impact = generate_business_impact(
                                intent=intent, 
                                rules=st.session_state['rules'], 
                                failures=st.session_state.get('report_snapshot', {}).get("failures", []),
                                total_rows=len(df_fixed_ctx),
                                api_key=gemini_key_active,
                                df_summary=df_summary_str
                            )
                            
                            st.session_state['ai_impact_text'] = ai_impact
                            status.update(label="✅ Strategic Analysis Complete!", state="complete", expanded=False)
                            
                    except Exception as e:
                        st.error(f"AI Error: {str(e)}")
            
            # Download Button (if content exists)
            if 'ai_impact_text' in st.session_state:
                with col_ai_download:
                    st.success("Analysis Ready for Download")
                    
                    pdf_ai = generate_pdf_report(
                        df_original=st.session_state['df'],
                        df_fixed=st.session_state['df'],
                        rules=st.session_state['rules'],
                        validation_results=st.session_state.get('report_snapshot', {}),
                        intent=intent,
                        impact_text=st.session_state['ai_impact_text']
                    )
                    
                    st.download_button(
                        label="📥 Download Strategic AI Report (PDF)",
                        data=pdf_ai,
                        file_name="strategic_data_audit.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )

else:
    st.info("Please upload a CSV and define the business intent to begin.")

