"""Streaming, self-contained HTML. No payload is inserted as markup or script."""
import html
import json
from decimal import Decimal

from .models import MISSING

QUALITY_FORMULA = (
    "Quality = value-matched unique eligible source IDs / all nonduplicated eligible source IDs * 100. "
    "Zero denominator = 100% (no unique expected records). Duplicates, extras and filter violations "
    "are separate failures; type-only discrepancies are warnings. Partial-run counts are provisional."
)


def display(value):
    def default(obj):
        if obj is MISSING:
            return {"representation": "MISSING"}
        if isinstance(obj, Decimal):
            return {"decimal": str(obj)}
        raise TypeError("Unsupported evidence value")
    return html.escape(json.dumps(value, ensure_ascii=True, indent=2, default=default), quote=True)


def write_html(result, output):
    """Write to an open text file incrementally; include every occurrence and error."""
    output.write('<!doctype html><html lang="en"><meta charset="utf-8">'
                 '<meta name="viewport" content="width=device-width">'
                 '<title>Data Certification Report</title><style>'
                 'body{font:15px system-ui;margin:2rem;color:#182333;background:#f5f7fb}'
                 'table{border-collapse:collapse;width:100%;background:white}'
                 'td,th{border:1px solid #b9c3d0;padding:.7rem;text-align:left;vertical-align:top}'
                 'pre{white-space:pre-wrap;overflow-wrap:anywhere;margin:.3rem 0}'
                 'details{margin:.7rem 0}summary{cursor:pointer;font-weight:bold}'
                 '</style><h1>Data Certification Report</h1>')
    output.write('<h2>Summary</h2><pre>' + display(result.summary()) + '</pre>')
    output.write('<p>' + html.escape(QUALITY_FORMULA) + '</p>')
    output.write('<p>Time boundaries: start inclusive, end exclusive. All evaluated records follow. '
                 'EXCLUDED source IDs must be absent from destination. EXTRA_DESTINATION_FIELD '
                 'lists unexpected destination paths.</p>')
    output.write('<h2>Records</h2><table><thead><tr><th>ID</th><th>Source Record</th>'
                 '<th>Destination Record</th><th>Comparison Result</th></tr></thead><tbody>')
    for evidence in result.index.evidence():
        output.write('<tr><td><pre>' + display(evidence["id"]) + '</pre></td>')
        for side in ("source", "destination"):
            output.write('<td>')
            count = 0
            for occurrence in result.index.occurrences(evidence["key"], side):
                count += 1
                output.write('<details open><summary>Occurrence ' + str(count) + '</summary><pre>')
                output.write(display(occurrence) + '</pre>')
                if occurrence["extra_fields"]:
                    output.write('<p>EXTRA_DESTINATION_FIELD</p><pre>' + display(occurrence["extra_fields"]) + '</pre>')
                output.write('</details>')
            if not count:
                output.write('NOT FOUND')
            output.write('</td>')
        output.write('<td><strong>' + html.escape(evidence["category"]) + '</strong>')
        if evidence["filter_violation"]:
            output.write('<p>FILTER_VIOLATION: an excluded source ID exists in destination.</p>')
        output.write('<pre>' + display({k: v for k, v in evidence.items() if k not in ("key", "id", "fields")}) + '</pre>')
        if evidence["fields"]:
            output.write('<details><summary>Field evidence (values and types)</summary><pre>')
            output.write(display(evidence["fields"]) + '</pre></details>')
        output.write('</td></tr>')
    output.write('</tbody></table><h2>Record errors</h2>')
    for error in result.index.errors():
        output.write('<pre>' + display(error) + '</pre>')
    output.write('</html>')
