"""
Institutional Streamlit Styling & Design System
===============================================
Provides dark-theme enterprise CSS styling, KPI metric cards, and badge indicators
inspired by Bloomberg Terminal and institutional risk platforms.
"""

CUSTOM_CSS = """
<style>
    /* Global Container Adjustments */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 100%;
    }

    /* Top Brand Header */
    .main-header {
        background: linear-gradient(135deg, #0d1b2a 0%, #1b263b 50%, #0d1b2a 100%);
        border: 1px solid #415a77;
        border-radius: 12px;
        padding: 20px 24px;
        margin-bottom: 24px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35);
    }
    .main-header h1 {
        color: #e0e1dd;
        font-size: 26px;
        font-weight: 700;
        margin: 0;
        letter-spacing: 0.5px;
    }
    .main-header p {
        color: #778da9;
        font-size: 13px;
        margin: 6px 0 0 0;
    }

    /* KPI Metric Card */
    .metric-card {
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 10px;
        padding: 16px 18px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s, border-color 0.2s;
    }
    .metric-card:hover {
        border-color: #38bdf8;
        transform: translateY(-2px);
    }
    .metric-title {
        color: #94a3b8;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 6px;
    }
    .metric-value {
        color: #f8fafc;
        font-size: 24px;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-subtitle {
        color: #64748b;
        font-size: 11px;
        margin-top: 4px;
    }
    .metric-delta-pos {
        color: #10b981;
        font-size: 12px;
        font-weight: 600;
    }
    .metric-delta-neg {
        color: #ef4444;
        font-size: 12px;
        font-weight: 600;
    }
    .metric-delta-neutral {
        color: #38bdf8;
        font-size: 12px;
        font-weight: 600;
    }

    /* Badges & Tags */
    .badge-critical {
        background-color: rgba(239, 68, 68, 0.15);
        color: #f87171;
        border: 1px solid #ef4444;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 700;
        display: inline-block;
    }
    .badge-high {
        background-color: rgba(245, 158, 11, 0.15);
        color: #fbbf24;
        border: 1px solid #f59e0b;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 700;
        display: inline-block;
    }
    .badge-watchlist {
        background-color: rgba(56, 189, 248, 0.15);
        color: #38bdf8;
        border: 1px solid #0284c7;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 700;
        display: inline-block;
    }
    .badge-normal {
        background-color: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid #10b981;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 700;
        display: inline-block;
    }

    /* Section Subheaders */
    .section-title {
        color: #e2e8f0;
        font-size: 16px;
        font-weight: 600;
        border-left: 4px solid #38bdf8;
        padding-left: 10px;
        margin-top: 16px;
        margin-bottom: 12px;
    }

    /* Callout Card */
    .callout-box {
        background: #0f172a;
        border-left: 4px solid #38bdf8;
        border-radius: 6px;
        padding: 14px 18px;
        margin: 12px 0;
        font-size: 13px;
        color: #cbd5e1;
    }

    /* Live Session Chip */
    .session-chip {
        background: rgba(16, 185, 129, 0.1);
        border: 1px solid #10b981;
        color: #34d399;
        border-radius: 16px;
        padding: 3px 10px;
        font-size: 11px;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
</style>
"""

def render_metric_card(title: str, value: str, subtitle: str = "", delta: str = "", delta_type: str = "neutral") -> str:
    """Generates HTML for an institutional KPI metric card."""
    delta_class = "metric-delta-neutral"
    if delta_type == "pos":
        delta_class = "metric-delta-pos"
    elif delta_type == "neg":
        delta_class = "metric-delta-neg"

    delta_html = f"<div class='{delta_class}'>{delta}</div>" if delta else ""
    sub_html = f"<div class='metric-subtitle'>{subtitle}</div>" if subtitle else ""

    return (
        f'<div class="metric-card">'
        f'<div class="metric-title">{title}</div>'
        f'<div class="metric-value">{value}</div>'
        f'{delta_html}'
        f'{sub_html}'
        f'</div>'
    )


def clean_html(html_str: str) -> str:
    """Strips leading whitespace from every line so Markdown parser will not treat it as a code block."""
    return "\n".join(line.strip() for line in html_str.strip().splitlines())


