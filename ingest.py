"""
Signal — Gmail Ingestion + Auto Digest Script
Fetches unread newsletters from Gmail, summarizes with Claude Haiku,
generates a monthly digest synthesis, and updates docs/ folder.

Daily run: fetches new emails, updates newsletters.json, regenerates digest
Month-end: archives previous month's digest to digest-YYYY-MM.json
"""

import imaplib
import email
import json
import os
import hashlib
import re
from datetime import datetime, timedelta
from email.header import decode_header
from bs4 import BeautifulSoup
import anthropic

# ── Config ──────────────────────────────────────────────────────────────────
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
DATA_PATH = "docs/newsletters.json"
DIGEST_PATH = "docs/digest-current.json"
MAX_CONTENT_CHARS = 4000
MAX_EMAILS = 30

# ── Newsletter source mapping ────────────────────────────────────────────────
SENDER_MAP = [
    # VC & Deal Flow
    {"match": "fortune.com",               "name": "Term Sheet",              "source": "Fortune / Dan Primack",  "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "US"},
    {"match": "axios.com",                 "name": "Pro Rata",                "source": "Axios / Dan Primack",    "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "US"},
    {"match": "pitchbook.com",             "name": "PitchBook",               "source": "PitchBook",              "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "Global"},
    {"match": "cbinsights.com",            "name": "CB Insights",             "source": "CB Insights",            "theme": "VC & Deal Flow",      "themeColor": "#E84B2E", "region": "Global"},
    # Macro & Strategy
    {"match": "thediff.co",                "name": "The Diff",                "source": "Byrne Hobart",           "theme": "Macro & Finance",     "themeColor": "#2563EB", "region": "US"},
    {"match": "notboring.co",              "name": "Not Boring",              "source": "Packy McCormick",        "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "US"},
    {"match": "generalist.com",            "name": "The Generalist",          "source": "Mario Gabriele",         "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "US"},
    {"match": "exponentialview.co",        "name": "Exponential View",        "source": "Azeem Azhar",            "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
    {"match": "ben-evans.com",             "name": "Benedict Evans",          "source": "Benedict Evans",         "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
    {"match": "stratechery.com",           "name": "Stratechery",             "source": "Ben Thompson",           "theme": "Strategy & Thesis",   "themeColor": "#0891B2", "region": "Global"},
    # Deep Tech
    {"match": "deeptechnewsletter.com",    "name": "Deep Tech Newsletter",    "source": "Deep Tech Newsletter",   "theme": "Deep Tech & Science", "themeColor": "#7C3AED", "region": "Global"},
    {"match": "defensetechnewsletter",     "name": "Defense Tech Newsletter", "source": "Defense Tech",           "theme": "Deep Tech & Science", "themeColor": "#7C3AED", "region": "US"},
    # China & Global
    {"match": "techbuzzchina.com",         "name": "Tech Buzz China",         "source": "Tech Buzz China",        "theme": "China & Global",      "themeColor": "#DC2626", "region": "China"},
    {"match": "technode.com",              "name": "TechNode",                "source": "TechNode",               "theme": "China & Global",      "themeColor": "#DC2626", "region": "China"},
    {"match": "caixinglobal.com",          "name": "Caixin Global",           "source": "Caixin",                 "theme": "China & Global",      "themeColor": "#DC2626", "region": "China"},
    # Europe
    {"match": "sifted.eu",                 "name": "Sifted",                  "source": "Sifted",                 "theme": "European VC",         "themeColor": "#059669", "region": "Europe"},
    {"match": "maddyness.com",             "name": "Maddyness",               "source": "Maddyness",              "theme": "European VC",         "themeColor": "#059669", "region": "Europe"},
    # Climate & Energy
    {"match": "heatmap.news",              "name": "Heatmap",                 "source": "Heatmap News",           "theme": "Climate & Energy",    "themeColor": "#D97706", "region": "Global"},
    {"match": "volts.wtf",                 "name": "Volts",                   "source": "David Roberts",          "theme": "Climate & Energy",    "themeColor": "#D97706", "region": "Global"},
    {"match": "latitudemedia.com",         "name": "Latitude Media",          "source": "Latitude Media",         "theme": "Climate & Energy",    "themeColor": "#D97706", "region": "Global"},
    # Fintech
    {"match": "fintechbusinessweekly",     "name": "Fintech Business Weekly", "source": "Jason Mikula",           "theme": "Fintech",             "themeColor": "#0D9488", "region": "Global"},
    {"match": "fintechblueprint",          "name": "Fintech Blueprint",       "source": "Lex Sokolin",            "theme": "Fintech",             "themeColor": "#0D9488", "region": "Global"},
]

SOURCE_URLS = {
    "Term Sheet": "https://fortune.com/section/term-sheet/",
    "Pro Rata": "https://www.axios.com/newsletters/axios-pro-rata",
    "PitchBook": "https://pitchbook.com/news/newsletters",
    "CB Insights": "https://www.cbinsights.com/newsletter",
    "The Diff": "https://www.thediff.co",
    "Not Boring": "https://www.notboring.co",
    "The Generalist": "https://www.generalist.com",
    "Exponential View": "https://www.exponentialview.co",
    "Benedict Evans": "https://www.ben-evans.com/newsletter",
    "Stratechery": "https://stratechery.com",
    "Deep Tech Newsletter": "https://deeptechnewsletter.com",
    "Defense Tech Newsletter": "https://defensetechnewsletter.curated.co",
    "Tech Buzz China": "https://www.techbuzzchina.com",
    "TechNode": "https://technode.com",
    "Caixin Global": "https://www.caixinglobal.com",
    "Sifted": "https://sifted.eu",
    "Maddyness": "https://www.maddyness.com",
    "Heatmap": "https://heatmap.news",
    "Volts": "https://www.volts.wtf",
    "Latitude Media": "https://www.latitudemedia.com",
    "Fintech Business Weekly": "https://fintechbusinessweekly.substack.com",
    "Fintech Blueprint": "https://www.fintechblueprint.com",
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def match_sender(from_addr):
    from_addr = from_addr.lower()
    for s in SENDER_MAP:
        if s["match"] in from_addr:
            return s
    return None

def decode_mime_header(value):
    if not value:
        return ""
    parts = decode_header(value)
    decoded = []
    for part, enc in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)

def extract_text(msg):
    text = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    soup = BeautifulSoup(html, "html.parser")
                    for tag in soup(["nav", "footer", "script", "style"]):
                        tag.decompose()
                    text = soup.get_text(separator=" ", strip=True)
                    break
                except Exception:
                    continue
            elif ct == "text/plain" and not text:
                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="replace")
                except Exception:
                    continue
    else:
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            raw = payload.decode(charset, errors="replace")
            if "<html" in raw.lower():
                soup = BeautifulSoup(raw, "html.parser")
                text = soup.get_text(separator=" ", strip=True)
            else:
                text = raw
        except Exception:
            pass
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:MAX_CONTENT_CHARS]

def month_key(date_str):
    return date_str[:7]  # "YYYY-MM"

def current_month_key():
    return datetime.now().strftime("%Y-%m")

def month_label(key):
    y, m = key.split("-")
    return datetime(int(y), int(m), 1).strftime("%B %Y")

# ── Article Summarization ────────────────────────────────────────────────────

def summarize(title, content, theme):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""You are an investment intelligence analyst. Analyze this newsletter email and respond in JSON only.

Newsletter: {title}
Theme: {theme}
Content:
{content}

IMPORTANT: First determine if this email contains substantive editorial/investment content worth summarizing.
Reject emails that are: welcome/confirmation emails, GDPR notices, privacy policy updates, unsubscribe confirmations, billing receipts, or any email with no actual editorial content.

Respond with ONLY valid JSON (no markdown, no explanation):
{{
  "is_substantive": true or false,
  "summary": "2-3 sentence summary focused on investment-relevant insights. Be specific about companies, figures, and trends mentioned. Write in clear journalistic prose without jargon.",
  "keyTakeaways": [
    "First sharp, actionable takeaway for a VC/PE investor — written in plain English",
    "Second takeaway — what does this mean for capital allocation?",
    "Third takeaway — macro or sector implication"
  ],
  "readTime": <estimated minutes to read the full article, integer>
}}

If is_substantive is false, still return the full JSON structure but with empty strings for the other fields."""

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        result = json.loads(raw)

        if not result.get("is_substantive", True):
            return None
        return result
    except Exception as e:
        print(f"  ⚠ Claude error: {e}")
        return {
            "is_substantive": True,
            "summary": content[:200] + "...",
            "keyTakeaways": ["Unable to generate AI summary — read original"],
            "readTime": 5
        }

# ── Digest Generation ────────────────────────────────────────────────────────

def generate_digest(articles, month_key_str):
    """Generate a synthesised monthly digest using Claude."""
    # Filter to substantive articles only
    good = [a for a in articles if a.get("summary") and "Unable to generate" not in a.get("summary","")]
    if not good:
        print("  ⚠ No substantive articles for digest")
        return None

    print(f"  ✦ Generating digest from {len(good)} articles...")

    # Group by theme
    by_theme = {}
    for n in good:
        if n["theme"] not in by_theme:
            by_theme[n["theme"]] = []
        by_theme[n["theme"]].append(n)

    theme_blocks = []
    for theme, items in by_theme.items():
        summaries = "\n".join([
            f"- {n['source']} ({n['date']}): {n['summary']} Key signals: {'; '.join(n.get('keyTakeaways', []))}"
            for n in items
        ])
        theme_blocks.append(f"THEME: {theme}\n{summaries}")

    prompt = f"""You are a senior investment analyst writing a monthly briefing for a global VC and private equity investor. You have read the following newsletter summaries from {month_label(month_key_str)}:

{chr(10).join(theme_blocks)}

Write a synthesised investment intelligence briefing. Do NOT simply summarise each article individually. Instead:

1. For each theme, write 2-3 paragraphs of analytical prose that identifies the common threads, emerging patterns, tensions, and contradictions ACROSS the articles in that theme. Write as a senior analyst would — with conviction, specific references to companies/figures mentioned, and investment implications.

2. After the theme sections, write a "So What?" section with 4-6 short paragraphs, each addressing one cross-theme macro signal or investment implication for a VC/PE investor. These should connect dots across themes.

Use clear, journalistic prose. No bullet points. No headers beyond the theme names and "So What?". Be specific — name companies, figures, and dates when relevant. Be analytical, not descriptive.

Respond in JSON only, no markdown:
{{
  "month": "{month_label(month_key_str)}",
  "generatedAt": "{datetime.now().isoformat()}",
  "articleCount": {len(good)},
  "intro": "2 sentence framing of this month's macro context",
  "themes": [
    {{
      "name": "theme name exactly as given",
      "color": "hex color for this theme",
      "synthesis": "full analytical paragraphs as a single string with \\n\\n between paragraphs",
      "sources": ["source1", "source2"]
    }}
  ],
  "sowhat": "4-6 short paragraphs as a single string with \\n\\n between paragraphs, each being a distinct cross-theme investment signal"
}}"""

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=8000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

        # Add theme colors from our map
        result = json.loads(raw)
        theme_colors = {n["theme"]: n["themeColor"] for n in good}
        for t in result.get("themes", []):
            if not t.get("color") or t["color"] == "hex color for this theme":
                t["color"] = theme_colors.get(t["name"], "#8B6914")

        print(f"  ✓ Digest generated: {len(result.get('themes',[]))} themes, {len(result.get('sowhat','').split(chr(10)+chr(10)))} insights")
        return result

    except Exception as e:
        print(f"  ⚠ Digest generation error: {e}")
        return None

# ── Gmail Fetch ──────────────────────────────────────────────────────────────

def fetch_gmail():
    print("📬 Connecting to Gmail...")
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
    mail.select("inbox")

    # Search all unread emails
    _, data = mail.search(None, 'UNSEEN')
    email_ids = data[0].split()

    print(f"  Found {len(email_ids)} unread emails")
    emails = []

    for eid in email_ids[-MAX_EMAILS:]:
        _, msg_data = mail.fetch(eid, "(RFC822)")
        msg = email.message_from_bytes(msg_data[0][1])

        from_addr = decode_mime_header(msg.get("From", ""))
        subject = decode_mime_header(msg.get("Subject", ""))
        date_str = msg.get("Date", "")

        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            date = dt.strftime("%Y-%m-%d")
        except Exception:
            date = datetime.now().strftime("%Y-%m-%d")

        source_meta = match_sender(from_addr)
        if not source_meta:
            print(f"  ↷ Unknown sender, skipping: {from_addr[:50]}")
            continue

        content = extract_text(msg)
        if len(content) < 100:
            print(f"  ↷ Too short, skipping: {subject[:50]}")
            continue

        emails.append({
            "subject": subject,
            "from": from_addr,
            "date": date,
            "content": content,
            "meta": source_meta,
            "email_id": eid.decode()
        })

    mail.logout()
    return emails

# ── Data I/O ─────────────────────────────────────────────────────────────────

def load_existing():
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH) as f:
            return json.load(f)
    return {"lastUpdated": None, "newsletters": [], "themes": []}

def save_data(data):
    os.makedirs("docs", exist_ok=True)
    data["lastUpdated"] = datetime.now().isoformat() + "Z"
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✓ Saved {len(data['newsletters'])} newsletters to {DATA_PATH}")

def save_digest(digest, month_key_str, is_archive=False):
    os.makedirs("docs", exist_ok=True)
    # Always save as current
    with open(DIGEST_PATH, "w") as f:
        json.dump(digest, f, indent=2)
    # If archiving, also save with month key
    if is_archive:
        archive_path = f"docs/digest-{month_key_str}.json"
        with open(archive_path, "w") as f:
            json.dump(digest, f, indent=2)
        print(f"✓ Archived digest to {archive_path}")
    print(f"✓ Saved digest to {DIGEST_PATH}")

def make_id(subject, date):
    return hashlib.md5(f"{subject}{date}".encode()).hexdigest()[:8]

# ── Main ─────────────────────────────────────────────────────────────────────

def ingest():
    print("🔄 Signal — Starting ingestion...")
    now = datetime.now()
    curr_month = current_month_key()
    is_last_day = (now + timedelta(days=1)).month != now.month

    existing = load_existing()
    existing_ids = {n["id"] for n in existing["newsletters"]}

    # Fetch and process new emails
    emails = fetch_gmail()
    new_items = []

    for e in emails:
        item_id = make_id(e["subject"], e["date"])
        if item_id in existing_ids:
            print(f"  ↷ Already processed: {e['subject'][:50]}")
            continue

        m = e["meta"]
        print(f"  ✦ Summarizing: {e['subject'][:60]}")

        ai = summarize(
            title=e["subject"],
            content=e["content"],
            theme=m["theme"]
        )

        if ai is None:
            print(f"  ✗ Skipped (not substantive): {e['subject'][:60]}")
            continue

        source_url = SOURCE_URLS.get(m["name"], "")

        new_items.append({
            "id": item_id,
            "title": m["name"],
            "source": m["source"],
            "date": e["date"],
            "theme": m["theme"],
            "themeColor": m["themeColor"],
            "region": m["region"],
            "summary": ai["summary"],
            "keyTakeaways": ai["keyTakeaways"],
            "url": source_url,
            "readTime": ai.get("readTime", 5),
            "originalTitle": e["subject"]
        })

    # Merge all articles (no cutoff — keep everything)
    combined = new_items + existing["newsletters"]
    combined.sort(key=lambda x: x["date"], reverse=True)

    # Rebuild themes
    theme_map = {}
    for n in combined:
        if n["theme"] not in theme_map:
            theme_map[n["theme"]] = {"name": n["theme"], "color": n["themeColor"], "count": 0}
        theme_map[n["theme"]]["count"] += 1

    save_data({"newsletters": combined, "themes": list(theme_map.values())})
    print(f"\n✅ Done. {len(new_items)} new items processed.")

    # Generate digest for current month
    this_month_articles = [n for n in combined if month_key(n["date"]) == curr_month]
    if this_month_articles:
        print(f"\n📊 Generating monthly digest ({len(this_month_articles)} articles)...")
        digest = generate_digest(this_month_articles, curr_month)
        if digest:
            # If last day of month, archive it
            save_digest(digest, curr_month, is_archive=is_last_day)
            if is_last_day:
                print(f"📁 Last day of month — digest archived as digest-{curr_month}.json")
    else:
        print("\n⚠ No articles this month yet — skipping digest generation")

if __name__ == "__main__":
    ingest()
