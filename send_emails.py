"""
Send daily vendor order emails via Outlook (Windows only).

For each vendor that has files in today's email staging folder, this script
creates one Outlook email with ALL of that vendor's PDFs attached — regardless
of which retailer (Depot, Lowe's, Tractor Supply) generated them.

Vendor email addresses are read from vendor_email_contacts.xlsx.

Usage
-----
  # Save to Drafts for review (safe default):
  python send_emails.py

  # Actually send immediately:
  python send_emails.py --send

  # Send files from a specific date instead of today:
  python send_emails.py --date 2026-03-20

  # Preview what would be sent without touching Outlook:
  python send_emails.py --dry-run

    # Clear all pending staged PDFs for a date without sending:
    python send_emails.py --clear-pending

Contact spreadsheet (vendor_email_contacts.xlsx)
-------------------------------------------------
Required columns:
  Vendor   – must match the vendor name exactly as it appears in the vendor maps
  Email    – primary To address (required; separate multiple with semicolons)
Optional columns:
  CC       – CC address(es), semicolon-separated
  BCC      – BCC address(es), semicolon-separated
  Subject  – custom subject line (leave blank to use the default)
    Body     – custom plain-text body (leave blank to use the default)
    LabelsFolder – optional folder path containing extra label PDFs to attach

Any vendor in the staging folder that has NO row in this file will be skipped
and listed in the summary at the end.
"""

import argparse
import datetime
import logging
import re
import sys
from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

EMAIL_STAGING_ROOT = Path("email_staging")
SENT_ARCHIVE_ROOT  = EMAIL_STAGING_ROOT / "sent"
SKIPPED_ARCHIVE_ROOT = EMAIL_STAGING_ROOT / "skipped"
DEFAULT_CONTACTS_XLSX = Path("vendor_email_contacts.xlsx")
ROUTES_XLSX_PATH = Path("Vendor Output Routes.xlsx")
ROUTES_PATH_COL_CANDIDATES = ["DestinationPath", "Path"]

FROM_NAME   = "Cornerstone Products"          # Display name shown in From field
DEFAULT_SUBJECT_TEMPLATE = "Your Orders – {vendor} – {date}"
DEFAULT_BODY_TEMPLATE = (
    "Hi,\n\n"
    "Please find attached your current packing slip order(s) for {date}.\n\n"
    "Retailers included in this email:\n{retailers}\n\n"
    "Thank you,\n{from_name}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("send_emails")


# ─────────────────────────────────────────────────────────────────────────────
# Contact list loader
# ─────────────────────────────────────────────────────────────────────────────

def load_contacts(xlsx_path: Path) -> dict[str, dict]:
    """Return {vendor_name: {email, cc, bcc, subject, body, labels_folder}} from the xlsx."""
    if not xlsx_path.exists():
        logger.error("Contact file not found: %s", xlsx_path)
        return {}
    try:
        df = pd.read_excel(xlsx_path, dtype=str).fillna("")
    except Exception as e:
        logger.error("Could not read %s: %s", xlsx_path, e)
        return {}

    # Normalise column names
    df.columns = [c.strip() for c in df.columns]

    required = {"Vendor", "Email"}
    missing = required - set(df.columns)
    if missing:
        logger.error("Contact file is missing columns: %s", missing)
        return {}

    contacts: dict[str, dict] = {}
    for _, row in df.iterrows():
        vendor = str(row.get("Vendor", "")).strip()
        email  = str(row.get("Email",  "")).strip()
        if not vendor or not email:
            continue
        contacts[vendor] = {
            "email":   email,
            "cc":      str(row.get("CC",      "")).strip(),
            "bcc":     str(row.get("BCC",     "")).strip(),
            "subject": str(row.get("Subject", "")).strip(),
            "body":    str(row.get("Body",    "")).strip(),
            "labels_folder": str(row.get("LabelsFolder", "")).strip(),
        }
    logger.info("Loaded %d vendor contacts from %s", len(contacts), xlsx_path)
    return contacts


def load_vendor_route_dirs(xlsx_path: Path) -> dict[str, set[Path]]:
    """Return {vendor_name: {route_dir, ...}} from Vendor Output Routes.xlsx."""
    if not xlsx_path.exists():
        logger.warning("Route workbook not found: %s", xlsx_path)
        return {}
    try:
        df = pd.read_excel(xlsx_path, dtype=str).fillna("")
    except Exception as e:
        logger.warning("Could not read route workbook %s: %s", xlsx_path, e)
        return {}

    df.columns = [c.strip() for c in df.columns]
    path_col = next((c for c in ROUTES_PATH_COL_CANDIDATES if c in df.columns), None)
    if path_col is None or "Vendor" not in df.columns:
        logger.warning("Route workbook missing required columns: Vendor and Path/DestinationPath")
        return {}

    result: dict[str, set[Path]] = {}
    for _, row in df.iterrows():
        vendor = str(row.get("Vendor", "")).strip()
        raw_path = str(row.get(path_col, "")).strip()
        if not vendor or not raw_path:
            continue
        result.setdefault(vendor, set()).add(Path(raw_path))
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Staging folder scanner
# ─────────────────────────────────────────────────────────────────────────────

def scan_staging(date_str: str) -> dict[str, list[Path]]:
    """Return {vendor_folder_name: [pdf_paths]} for the given date."""
    day_dir = EMAIL_STAGING_ROOT / date_str
    if not day_dir.exists():
        logger.warning("No staging folder found for %s (%s)", date_str, day_dir)
        return {}

    result: dict[str, list[Path]] = {}
    for vendor_dir in sorted(day_dir.iterdir()):
        if not vendor_dir.is_dir():
            continue
        pdfs = sorted(vendor_dir.glob("*.pdf"))
        if pdfs:
            result[vendor_dir.name] = pdfs

    logger.info("Found %d vendor(s) with staged PDFs for %s", len(result), date_str)
    return result


def _folder_name_to_vendor(folder_name: str, contacts: dict[str, dict]) -> str | None:
    """Try to match a staging folder name back to a contact vendor name.

    The staging folder name is the safe (filesystem-sanitised) version of the
    vendor name.  We try an exact match first, then a normalise-and-compare.
    """
    if folder_name in contacts:
        return folder_name

    def _norm(s: str) -> str:
        return re.sub(r"[^\w]", "", s).lower()

    norm_folder = _norm(folder_name)
    for vendor in contacts:
        if _norm(vendor) == norm_folder:
            return vendor

    # Flexible fallback for small naming differences:
    # "Agra" in sheet vs "Agra Life" in staged folder (or vice versa).
    partial_matches = [
        vendor for vendor in contacts
        if _norm(vendor) in norm_folder or norm_folder in _norm(vendor)
    ]
    if len(partial_matches) == 1:
        return partial_matches[0]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Outlook email creation
# ─────────────────────────────────────────────────────────────────────────────

def _build_retailer_list(pdfs: list[Path]) -> str:
    """Best-effort retailer extraction from attachment file names."""
    retailers: list[str] = []
    for p in pdfs:
        stem_lower = p.stem.lower()
        if "home depot" in stem_lower and "Home Depot" not in retailers:
            retailers.append("Home Depot")
        if "lowe" in stem_lower and "Lowe's" not in retailers:
            retailers.append("Lowe's")
        if "tractor supply" in stem_lower and "Tractor Supply" not in retailers:
            retailers.append("Tractor Supply")
    return "\n".join(f"  • {r}" for r in retailers) if retailers else "  • See attached"


def collect_vendor_attachments(
    vendor: str,
    contact: dict,
    staged_order_pdfs: list[Path],
    route_dirs_by_vendor: dict[str, set[Path]],
) -> tuple[list[Path], int]:
    """Return (attachments, extra_count) for staged orders plus label PDFs.

    Label source order:
    1. LabelsFolder from the contacts sheet, if set
    2. Otherwise, the vendor's normal output folder(s) from Vendor Output Routes.xlsx

    In same-folder mode, only PDFs with a 7- or 8-digit filename stem are
    treated as labels, matching the PO-number naming convention.
    """
    attachments: list[Path] = list(staged_order_pdfs)
    labels_folder_raw = (contact.get("labels_folder") or "").strip()
    candidate_dirs: list[Path] = []
    same_folder_mode = False
    if labels_folder_raw:
        candidate_dirs.append(Path(labels_folder_raw))
    else:
        candidate_dirs.extend(sorted(route_dirs_by_vendor.get(vendor, set()), key=lambda p: str(p).lower()))
        same_folder_mode = True

    if not candidate_dirs:
        return attachments, 0

    staged_set = {p.resolve() for p in staged_order_pdfs if p.exists()}
    staged_names = {p.name.lower() for p in staged_order_pdfs}
    order_start = min((p.stat().st_mtime for p in staged_order_pdfs if p.exists()), default=0.0) - 10 * 60
    extra: list[Path] = []
    for labels_dir in candidate_dirs:
        if not labels_dir.exists() or not labels_dir.is_dir():
            logger.warning("Labels folder does not exist or is not a directory: %s", labels_dir)
            continue

        for p in sorted(labels_dir.glob("*.pdf")):
            try:
                rp = p.resolve()
                if rp in staged_set:
                    continue
                if p.name.lower() in staged_names:
                    continue
                if same_folder_mode and not re.fullmatch(r"\d{7,8}", p.stem):
                    continue
                if same_folder_mode and p.stat().st_mtime < order_start:
                    continue
                extra.append(p)
            except OSError:
                continue

    # Keep deterministic order for labels (oldest -> newest).
    extra = sorted({p.resolve(): p for p in extra}.values(), key=lambda x: (x.stat().st_mtime, x.name.lower()))
    attachments.extend(extra)
    if extra:
        source_desc = "vendor output folder" if same_folder_mode else "LabelsFolder"
        logger.info("Including %d extra label PDF(s) for %s from %s", len(extra), vendor, source_desc)
    return attachments, len(extra)


def archive_sent_attachments(date_str: str, vendor_folder: str, pdfs: list[Path]) -> None:
    """Move successfully sent attachments out of staging so they cannot resend."""
    dest_dir = SENT_ARCHIVE_ROOT / date_str / vendor_folder
    dest_dir.mkdir(parents=True, exist_ok=True)

    for src in pdfs:
        if not src.exists():
            continue
        dest = dest_dir / src.name
        # Overwrite existing archive copy if present.
        if dest.exists():
            dest.unlink()
        src.replace(dest)

    # Remove now-empty vendor folder from staging.
    src_vendor_dir = EMAIL_STAGING_ROOT / date_str / vendor_folder
    try:
        if src_vendor_dir.exists() and not any(src_vendor_dir.iterdir()):
            src_vendor_dir.rmdir()
    except OSError:
        pass


def archive_skipped_attachments(date_str: str, vendor_folder: str, pdfs: list[Path]) -> None:
    """Move skipped vendor PDFs out of staging so they cannot be sent later."""
    dest_dir = SKIPPED_ARCHIVE_ROOT / date_str / vendor_folder
    dest_dir.mkdir(parents=True, exist_ok=True)

    for src in pdfs:
        if not src.exists():
            continue
        dest = dest_dir / src.name
        if dest.exists():
            dest.unlink()
        src.replace(dest)

    src_vendor_dir = EMAIL_STAGING_ROOT / date_str / vendor_folder
    try:
        if src_vendor_dir.exists() and not any(src_vendor_dir.iterdir()):
            src_vendor_dir.rmdir()
    except OSError:
        pass


def archive_pending_attachments(date_str: str, staged: dict[str, list[Path]]) -> int:
    """Move all active staged PDFs for the date into skipped archive.

    This is a manual sanity-reset command so pending items cannot be sent later.
    Returns the number of moved PDFs.
    """
    moved = 0
    for vendor_folder, pdfs in staged.items():
        archive_skipped_attachments(date_str, vendor_folder, pdfs)
        moved += len([p for p in pdfs if p.exists()])
    return moved


def create_outlook_email(
    vendor: str,
    contact: dict,
    pdfs: list[Path],
    date_str: str,
    draft_only: bool,
    dry_run: bool,
) -> bool:
    """Create (and optionally send) one Outlook email for a vendor.

    Returns True on success, False on failure.
    """
    formatted_date = datetime.date.fromisoformat(date_str).strftime("%B %d, %Y")
    retailer_list  = _build_retailer_list(pdfs)

    subject = contact["subject"] or DEFAULT_SUBJECT_TEMPLATE.format(
        vendor=vendor, date=formatted_date
    )
    body = contact["body"] or DEFAULT_BODY_TEMPLATE.format(
        vendor=vendor,
        date=formatted_date,
        retailers=retailer_list,
        from_name=FROM_NAME,
    )

    if dry_run:
        mode = "DRAFT" if draft_only else "SEND"
        logger.info(
            "[DRY-RUN] Would %s to %s (%s): %d attachment(s) — %s",
            mode, vendor, contact["email"], len(pdfs),
            ", ".join(p.name for p in pdfs),
        )
        return True

    try:
        import win32com.client as win32  # type: ignore[import]
    except ImportError:
        logger.error(
            "pywin32 is not installed. Run:  pip install pywin32\n"
            "This script must be run on a Windows machine with Outlook installed."
        )
        return False

    try:
        outlook = win32.Dispatch("Outlook.Application")
        mail    = outlook.CreateItem(0)   # 0 = olMailItem

        mail.Subject = subject
        mail.To      = contact["email"]
        if contact["cc"]:
            mail.CC = contact["cc"]
        if contact["bcc"]:
            mail.BCC = contact["bcc"]
        mail.Body = body

        missing: list[str] = []
        for pdf in pdfs:
            resolved = pdf.resolve()
            if resolved.exists():
                mail.Attachments.Add(str(resolved))
            else:
                missing.append(pdf.name)
                logger.warning("[%s] Attachment not found: %s", vendor, pdf)

        if missing:
            mail.Body += (
                f"\n\n[Note: {len(missing)} file(s) could not be attached: "
                + ", ".join(missing)
                + "]"
            )

        if draft_only:
            mail.Save()
            logger.info(
                "[DRAFT] Saved draft for %s (%s): %d attachment(s)",
                vendor, contact["email"], len(pdfs),
            )
        else:
            mail.Send()
            logger.info(
                "[SENT] Emailed %s (%s): %d attachment(s) — %s",
                vendor, contact["email"], len(pdfs),
                ", ".join(p.name for p in pdfs),
            )

        return True

    except Exception as e:
        logger.error("[%s] Outlook error: %s", vendor, e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send daily vendor order emails via Outlook."
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually send emails (default is to save as Drafts).",
    )
    parser.add_argument(
        "--date",
        default=datetime.date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Process staging files for this date (default: today).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without touching Outlook.",
    )
    parser.add_argument(
        "--contacts",
        default="",
        help="Optional path to the contacts workbook. Defaults to 'vendor_email_contacts.xlsx'.",
    )
    parser.add_argument(
        "--clear-pending",
        action="store_true",
        help="Archive all active staged PDFs for the date without sending.",
    )
    args = parser.parse_args()

    draft_only = not args.send
    mode_label = "DRY-RUN" if args.dry_run else ("DRAFT" if draft_only else "LIVE SEND")
    logger.info("=== Vendor Email Dispatch — %s — Mode: %s ===", args.date, mode_label)

    contacts_path = Path(args.contacts) if args.contacts else DEFAULT_CONTACTS_XLSX

    contacts = load_contacts(contacts_path)
    if not contacts and not args.dry_run:
        logger.error("No contacts loaded. Create/fix %s first.", contacts_path)
        sys.exit(1)
    route_dirs_by_vendor = load_vendor_route_dirs(ROUTES_XLSX_PATH)

    staged = scan_staging(args.date)
    if not staged:
        logger.info("Nothing to send for %s.", args.date)
        return

    if args.clear_pending:
        pending_count = sum(len(pdfs) for pdfs in staged.values())
        for vendor_folder, pdfs in staged.items():
            archive_skipped_attachments(args.date, vendor_folder, pdfs)
        logger.info(
            "Archived %d pending staged PDF(s) for %s. Nothing was sent.",
            pending_count,
            args.date,
        )
        return

    sent_ok:    list[str] = []
    sent_fail:  list[str] = []
    skipped:    list[str] = []
    detail_lines: list[str] = []

    for folder_name, staged_order_pdfs in staged.items():
        vendor = _folder_name_to_vendor(folder_name, contacts)
        if vendor is None:
            logger.warning(
                "No contact found for vendor folder '%s' — skipping.", folder_name
            )
            if not args.dry_run:
                archive_skipped_attachments(args.date, folder_name, staged_order_pdfs)
                logger.info(
                    "Archived skipped PDFs for vendor folder '%s' so they cannot send later.",
                    folder_name,
                )
            detail_lines.append(f"SKIPPED  | {folder_name} | {len(staged_order_pdfs)} staged PDF(s) | no contact row")
            skipped.append(folder_name)
            continue

        contact = contacts[vendor]
        attachments, extra_count = collect_vendor_attachments(vendor, contact, staged_order_pdfs, route_dirs_by_vendor)
        detail_lines.append(
            f"READY    | {vendor} | {len(staged_order_pdfs)} order PDF(s), {extra_count} label PDF(s) | {len(attachments)} total attachment(s)"
        )
        ok = create_outlook_email(
            vendor=vendor,
            contact=contact,
            pdfs=attachments,
            date_str=args.date,
            draft_only=draft_only,
            dry_run=args.dry_run,
        )
        if ok and args.send and not args.dry_run:
            # Archive only staged order PDFs; label files stay in their own folder.
            archive_sent_attachments(args.date, folder_name, staged_order_pdfs)
            logger.info("Archived sent attachments for %s so they cannot resend.", vendor)
        (sent_ok if ok else sent_fail).append(vendor)

    # Summary
    logger.info("")
    logger.info("─── Summary ───")
    for line in detail_lines:
        logger.info("  %s", line)
    logger.info("  Succeeded : %d — %s", len(sent_ok),   ", ".join(sent_ok)   or "none")
    logger.info("  Failed    : %d — %s", len(sent_fail), ", ".join(sent_fail) or "none")
    logger.info("  Skipped   : %d — %s", len(skipped),   ", ".join(skipped)   or "none")

    if skipped:
        logger.info("")
        logger.info(
            "Add the following names (or their sanitised equivalents) to %s:",
            contacts_path,
        )
        for s in skipped:
            logger.info("  %s", s)

    if sent_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
