# ctxt_generator.py

import os
import re
import json
import textwrap
from glob import glob
from bs4 import BeautifulSoup, NavigableString, Tag
from dotenv import load_dotenv
from typing import List, Tuple

load_dotenv()
EXPORT_DIR = os.getenv("EXPORT_DIR", "sharepoint_exports")
MAX_CHARS  = int(os.getenv("MAX_CHUNK_CHARS", 500))
RUN_TAG    = os.getenv("RUN_TAG", "").strip()

# ------------ Helpers ------------
def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def norm_multiline(s: str) -> str:
    """Normalize whitespace on each line but preserve explicit newlines."""
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = s.split("\n")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in lines]
    # keep blank lines if they were intentional (rare); collapse multiple blanks to single
    out = []
    prev_blank = False
    for ln in lines:
        if ln == "":
            if not prev_blank:
                out.append("")
            prev_blank = True
        else:
            out.append(ln)
            prev_blank = False
    return "\n".join(out).strip()

def is_bold_heading_p(p: Tag) -> bool:
    if not (p and p.name == "p"):
        return False
    strongs = p.find_all("strong", recursive=False)
    if not strongs:
        return False
    for child in p.children:
        if isinstance(child, NavigableString):
            if norm_ws(str(child)) not in ("", "\xa0"):
                return False
        elif isinstance(child, Tag):
            if child.name not in ("strong", "br"):
                if not (child.name == "u" and child.parent and child.parent.name == "strong"):
                    return False
    return True

def extract_plain_text(el: Tag) -> str:
    for a in el.find_all("a", href=True):
        a.replace_with(f"{a.get_text(' ', strip=True)} ({a['href']})")
    return norm_ws(el.get_text(" "))

def shorten_label(s: str, limit: int = 120) -> str:
    s = norm_ws(s)
    if len(s) <= limit:
        return s
    m = re.search(r"[,;:\u2013\u2014\-]\s", s[:limit][::-1])
    if m:
        cut = limit - m.start()
        return norm_ws(s[:cut]) + "…"
    return s[:limit].rstrip() + "…"

# --- Q/A detection ---
def is_question_text(t: str) -> bool:
    return bool(re.match(r'^\s*[Qq]\s*[:.\-]\s+', t))

def is_answer_text(t: str) -> bool:
    return bool(re.match(r'^\s*[Aa]\s*[:.\-]\s+', t))

def strip_q_or_a_prefix(t: str) -> str:
    return re.sub(r'^\s*[QqAa]\s*[:.\-]\s+', '', t, count=1).strip()

def list_to_lines(root_list: Tag) -> List[str]:
    lines: List[str] = []
    def walk_list(ul: Tag, indent: int = 0):
        for li in ul.find_all("li", recursive=False):
            li_clone = BeautifulSoup(str(li), "html.parser")
            for sub in li_clone.find_all(["ul", "ol"]):
                sub.decompose()
            txt = extract_plain_text(li_clone)
            bullet = ("  " * indent) + "- " + txt if txt else ""
            if bullet:
                lines.append(bullet)
            for sub_list in li.find_all(["ul", "ol"], recursive=False):
                walk_list(sub_list, indent + 1)
    walk_list(root_list, 0)
    return lines

# ------------ Outline builder ------------
def build_blocks_from_html(html: str) -> List[Tuple[List[str], str]]:
    soup = BeautifulSoup(html, "html.parser")

    # Convert tables to simple paragraphs
    for table in soup.find_all("table"):
        lines = []
        for tr in table.find_all("tr"):
            cells = [norm_ws(td.get_text(" ")) for td in tr.find_all(["th", "td"])]
            if cells:
                lines.append(" | ".join(cells))
        table.replace_with(BeautifulSoup("<p>" + "<br/>".join(lines) + "</p>", "html.parser"))

    blocks: List[Tuple[List[str], str]] = []
    breadcrumb: List[str] = []
    skipped_ids = set()

    def push_block(text: str):
        # Preserve explicit newlines if present (for Q/A blocks)
        t = norm_multiline(text) if ("\n" in (text or "")) else norm_ws(text)
        if t:
            blocks.append((breadcrumb.copy(), t))

    def emit_list_blocks(ul: Tag, trail: List[str]):
        for li in ul.find_all("li", recursive=False):
            li_clone = BeautifulSoup(str(li), "html.parser")
            for sub in li_clone.find_all(["ul", "ol"]):
                sub.decompose()
            li_text = extract_plain_text(li_clone)
            blocks.append((trail.copy(), f"- {li_text}"))
            for sub_list in li.find_all(["ul", "ol"], recursive=False):
                sub_label = shorten_label(li_text, 80)
                emit_list_blocks(sub_list, trail + [sub_label])

    def collect_answer_content(start: Tag, current_breadcrumb: List[str]) -> Tuple[str, List[int]]:
        consumed = []
        lines: List[str] = []
        node = start

        while node and node.next_sibling:
            node = node.next_sibling

            if isinstance(node, NavigableString):
                if not norm_ws(str(node)):
                    continue
                continue

            if not isinstance(node, Tag):
                continue

            if id(node) in skipped_ids:
                continue

            if node.name == "p" and is_bold_heading_p(node):
                heading_text = extract_plain_text(node)
                heading_text = re.sub(r"\s*:\s*$", "", heading_text)
                if current_breadcrumb and norm_ws(heading_text) == norm_ws(current_breadcrumb[0]):
                    consumed.append(id(node))
                    skipped_ids.add(id(node))
                    node.extract()
                    continue
                else:
                    break

            if node.name == "p":
                txt = extract_plain_text(node)
                if is_question_text(txt):
                    break
                if is_answer_text(txt):
                    txt = strip_q_or_a_prefix(txt)
                if norm_ws(txt):
                    lines.append(txt)
                consumed.append(id(node))
                skipped_ids.add(id(node))
                node.extract()
                continue

            if node.name in ("ul", "ol"):
                list_lines = list_to_lines(node)
                if list_lines:
                    lines.extend(list_lines)
                consumed.append(id(node))
                skipped_ids.add(id(node))
                node.extract()
                continue

            if node.name in ("table",):
                break

            break

        answer_text = "\n".join(lines).strip()
        return (answer_text, consumed)

    for node in soup.body.find_all(recursive=False) if soup.body else soup.find_all(recursive=False):
        stack = [node]
        while stack:
            el = stack.pop(0)
            if isinstance(el, NavigableString) or not isinstance(el, Tag):
                continue
            if id(el) in skipped_ids:
                continue

            if el.name == "p" and is_bold_heading_p(el):
                heading_text = extract_plain_text(el)
                heading_text = re.sub(r"\s*:\s*$", "", heading_text)
                breadcrumb = [heading_text]
                continue

            if el.name in ("ul", "ol"):
                emit_list_blocks(el, breadcrumb.copy())
                continue

            if el.name == "p":
                p_txt = extract_plain_text(el)
                if is_question_text(p_txt):
                    q_text = strip_q_or_a_prefix(p_txt)
                    a_text, _ = collect_answer_content(el, breadcrumb.copy())
                    if a_text:
                        push_block(f"Q: {q_text}\nA: {a_text}")  # keep explicit newline
                    else:
                        push_block(f"Q: {q_text}")
                    skipped_ids.add(id(el))
                    continue

                if norm_ws(p_txt):
                    push_block(p_txt)
                continue

            stack = list(el.children) + stack

    return blocks

# ------------ Chunker ------------
def chunk_blocks(blocks, limit: int = MAX_CHARS):
    chunks = []
    current_lines = []
    current_breadcrumb = None
    current_len = 0

    def breadcrumb_header(path):
        return " > ".join(shorten_label(p, 80) for p in path) if path else ""

    def start_new_chunk(path):
        nonlocal current_lines, current_len, current_breadcrumb
        if current_lines:
            chunks.append("\n".join(current_lines).strip())
        current_lines = []
        current_breadcrumb = path
        hdr = breadcrumb_header(path)
        if hdr:
            line = f"### {hdr}"
            current_lines.append(line)
            return len(line) + 1
        return 0

    def add_line(seg: str, path):
        nonlocal current_len
        if current_len + len(seg) + 1 > limit:
            # start new chunk under the same breadcrumb
            start_new_chunk(path)
            current_len = len(current_lines[0]) + 1 if current_lines else 0  # header length
        current_lines.append(seg)
        current_len += len(seg) + 1

    for path, text in blocks:
        if current_breadcrumb != path:
            current_len = start_new_chunk(path)

        is_qa_block = (text.lstrip().startswith("Q:") and "\nA:" in text)
        if is_qa_block and (current_len + len(text) + 1 > limit):
            current_len = start_new_chunk(path)

        # Respect explicit newlines: add each line separately
        if "\n" in text:
            for ln in text.split("\n"):
                ln = ln.rstrip()
                # do NOT wrap QA lines unless a single line exceeds limit badly
                if len(ln) + current_len > limit:
                    # if too long (e.g., a long URL), wrap conservatively
                    wrapped = textwrap.wrap(ln, width=max(40, limit - 20)) or [ln]
                    for w in wrapped:
                        add_line(w, path)
                else:
                    add_line(ln, path)
            continue

        # Single-line content (paragraphs, list items)
        line = text
        if len(line) + current_len > limit:
            bullet_prefix = ""
            m = re.match(r"^(\s*-\s+)", line)
            if m:
                bullet_prefix = m.group(1)
            wrapped = textwrap.wrap(line, width=max(40, limit - 20)) or [line]
            for w in wrapped:
                seg = (bullet_prefix + w) if (bullet_prefix and not w.startswith(bullet_prefix)) else w
                add_line(seg, path)
        else:
            add_line(line, path)

    if current_lines:
        chunks.append("\n".join(current_lines).strip())
    return chunks

# ------------ Main ------------
def process_files():
    html_files = glob(os.path.join(EXPORT_DIR, "*.html"))
    for html_path in html_files:
        base_name  = os.path.splitext(os.path.basename(html_path))[0]
        ctxt_path  = os.path.join(EXPORT_DIR, f"{base_name}.ctxt")
        meta_path  = os.path.join(EXPORT_DIR, f"{base_name}.meta.json")

        url_line = ""
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as mf:
                meta = json.load(mf)
                if "url" in meta:
                    url_line = f"`source_url: {meta['url']}`\n"

        with open(html_path, "r", encoding="utf-8") as f:
            raw = f.read()

        blocks = build_blocks_from_html(raw)
        chunks = chunk_blocks(blocks, limit=MAX_CHARS)

        tags_value = "policies" + (", " + RUN_TAG if RUN_TAG else "")
        with open(ctxt_path, "w", encoding="utf-8") as out:
            out.write("`version: 1`\n")
            out.write(f"`title: {base_name}`\n")
            out.write(f"`tags: [{tags_value}]`\n")
            if url_line:
                out.write(url_line)
            out.write("\n")
            for c in chunks:
                out.write(c.strip() + "\n\n")

        print(f"📝 Generated: {ctxt_path} ({len(chunks)} chunks)")

if __name__ == "__main__":
    process_files()
