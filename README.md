# 🍺 PubPulse Review Analyser

**A Python-based tool that turns Google Reviews into actionable business insights using the OpenAI API.**

---

## 🧩 Overview

This personal project explores how AI can be used to transform unstructured customer feedback into meaningful management reports.

The app uses the **Google Reviews API (via SerpAPI)** to collect all customer reviews for pubs under the **Green King** chain, then processes and analyses them through the **OpenAI SDK** to extract sentiment, category scores, and detailed summaries.

Finally, results are visualised in **Power BI** for easy comparison across pubs.

---

## 🏗️ Architecture

1. **SerpAPI** – fetches Google reviews as JSON files.  
2. **OpenAI SDK** – analyses reviews and generates category scores (service, food, ambience, etc.).  
3. **Python Scripts** – automate data fetching, processing, and analysis.  
4. **Power BI** – visualises insights for management decision-making.
