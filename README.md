# HolocronAI 🌌
**The Ultimate All-in-One AI Ecosystem for Telegram Developers and Power Users.**

HolocronAI is a high-performance framework built on **Aiogram 3.x**, designed to bridge the gap between Large Language Models and real-world execution. It’s not just a chatbot; it's a deployment engine, a document processor, and a coding assistant.

---

## 🔥 Key Features

### 🚀 One-Click Deployment & Web
* **Instant Bot Sandbox:** Deploy and test Telegram bots with a single button click.
* **Auto-Correction Engine:** Built-in system that automatically identifies and fixes code errors in real-time.
* **Temporary Web Hosting:** Launch your web projects onto temporary URLs for instant preview.

### 🧠 Advanced AI & Coding (Claude-style)
* **Interactive Code Generation:** Multi-step clarifying polls before generation to ensure 100% accuracy (similar to Claude's workflow).
* **Batch or Unit Generation:** Generate entire project structures or single files.
* **GitHub Scanner:** Deep analysis of repositories with RAG integration for codebase Q&A.
* **Bring Your Own Key (BYOK):** Seamlessly connect OpenAI, Claude (Anthropic), DeepSeek, Gemini, and more.
* **Streaming UI:** Live "typing" responses for a smooth user experience.

### 📂 Multimedia & Document Intelligence
* **Archive Architect:** Deep-read and process ZIP, RAR, 7z, and TAR archives.
* **Vision & Audio:** OCR for text recognition from photos and high-accuracy voice-to-text transcription.
* **Web Scraping:** Instant content parsing from any URL.
* **PPTX Generator:** Automated creation of professional presentations.
* **Telegra.ph Integration:** Instant publishing of long-form AI-generated articles.

### 💳 Commercial & Ecosystem
* **Full-Scale Billing:** Integrated payments via SBP, RU Cards, and CryptoBot.
* **Referral System:** Earn 5% from every referral purchase.
* **Payouts:** Flexible withdrawal of referral balance to Cards or USDT.
* **Session Isolation:** Isolated chat histories with full export capabilities.
* **Internal Token Economy:** Scalable balance management for users.

---

## 🛠 Tech Stack

* **Core:** Python 3.10+, Aiogram 3.x
* **Infrastructure:** Docker API (for dynamic container management), Nginx
* **Data:** PostgreSQL (Async), Redis (FSM & Rate-limiting), ClickHouse (RAG Vector Search)
* **AI Models:** Integrated support for OpenAI, OpenRouter, VseGPT, Ollama, and Google Gemini
* **Formatting:** Rich HTML output with interactive inline buttons

---

## ⚙️ Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/sntroop/HolocronAI.git](https://github.com/sntroop/HolocronAI.git)
   cd HolocronAI

 * Configure Environment:
   Use .env.example to create your .env file. Fill in your API tokens and database credentials.
 * Deploy with Docker:
   docker-compose up -d --build

🛡 Security & Reliability
HolocronAI includes a custom security layer:
 * Anti-Flood & Rate Limiting: Global and action-specific protection.
 * Docker Isolation: User-deployed bots run in restricted environments.
 * Callback Validation: Robust protection against unauthorized data injection.
Developed by sntroop Building high-load AI systems for the Telegram ecosystem.
