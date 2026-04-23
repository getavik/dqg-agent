# 🛡️ Governance Bridge - SaaS Deployment Guide

This agent is packaged as a portable Docker container, making it ready for instant SaaS deployment.

## 🚀 Quick Start (Local)

1. **Build the Image**
   ```bash
   docker build -t dqg-agent .
   ```

2. **Run the SaaS**
   ```bash
   docker run -p 8501:8501 dqg-agent
   ```
   Open `http://localhost:8501` in your browser.

## ☁️ Deploying to Cloud (SaaS)

### Option 1: Streamlit Community Cloud (Easiest)
1. Push this code to a public GitHub repository.
2. Go to [share.streamlit.io](https://share.streamlit.io).
3. Connect your repo and click **Deploy**.
4. **Secrets**: In the dashboard settings, add your `GEMINI_API_KEY` (optional, users can also enter it in the UI).

### Option 2: Docker Hosting (Render, Railway, AWS)
1. **Render.com**:
   - Select "New Web Service" > "Build from Docker".
   - Connect your repo.
   - Render will auto-detect the `Dockerfile` and build it.
2. **Google Cloud Run**:
   ```bash
   gcloud run deploy dqg-agent --source .
   ```

## 🔐 Configuration
- **Gemini API Key**: The agent currently asks for this in the secure sidebar for maximum portability (BYOK - Bring Your Own Key model).
- To pre-configure it for a private enterprise deployment, set `GEMINI_API_KEY` as an environment variable and update `app.py` to read it: `os.environ.get("GEMINI_API_KEY")`.
