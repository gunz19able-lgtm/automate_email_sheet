from scraper import collect_multiple_league_data, save_batch_to_excel
from google_sheet_automation import save_batch_to_google_sheets, compare_player_urls, save_players_to_google_sheets
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from typing import List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from logger import setup_logger
from dotenv import load_dotenv
from typing import Dict
from zoneinfo import ZoneInfo
from datetime import datetime
from email import encoders
import mimetypes
import asyncio
import smtplib
import os


logger = asyncio.run(setup_logger('email automation'))


# Load environment variables
load_dotenv()
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv("EMAIL_PASS")
CLIENT_NAME = os.getenv('CLIENT_NAME')

logger = asyncio.run(setup_logger('tennis_automation'))



async def send_email_with_attachment(
    smtp_server: str,
    smtp_port: int,
    smtp_username: str,
    smtp_password: str,
    from_email: str,
    to_emails: List[str],
    subject: str,
    body: str,
    attachment_path: str,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    text_body: Optional[str] = None,
    is_html: bool = False
) -> bool:

    try:
        # Create message container
        msg = MIMEMultipart('mixed')
        msg['From'] = from_email
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject

        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)

        # Create text content container
        text_msg = MIMEMultipart('alternative')

        # Add text parts
        if is_html and text_body:
            text_msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
            text_msg.attach(MIMEText(body, 'html', 'utf-8'))
        elif is_html:
            text_msg.attach(MIMEText(body, 'html', 'utf-8'))
        else:
            text_msg.attach(MIMEText(body, 'plain', 'utf-8'))

        msg.attach(text_msg)

        # Handle attachment - SINGLE ATTACHMENT ONLY
        if attachment_path and os.path.exists(attachment_path):
            # Get the actual filename (like "Lunar Ligaen - Efterår 2025_2025-09-09_22:44:03.xlsx")
            filename = os.path.basename(attachment_path)

            # Read the file
            with open(attachment_path, "rb") as f:
                attachment_data = f.read()

            # Create the attachment part
            attachment_part = MIMEApplication(attachment_data)

            # Set the proper filename in Content-Disposition header
            attachment_part.add_header(
                'Content-Disposition',
                'attachment',
                filename=filename  # This preserves your timestamped filename
            )

            # Set Content-Type for Excel files
            if filename.endswith('.xlsx'):
                attachment_part.add_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            elif filename.endswith('.xls'):
                attachment_part.add_header('Content-Type', 'application/vnd.ms-excel')

            # Attach to message - ONLY ONCE
            msg.attach(attachment_part)

            print(f"Attached file: {filename}")

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)

            recipients = to_emails.copy()
            if cc_emails:
                recipients.extend(cc_emails)
            if bcc_emails:
                recipients.extend(bcc_emails)

            server.sendmail(from_email, recipients, msg.as_string())

        print(f"Email sent successfully to {len(recipients)} recipient(s)")
        return True

    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False


async def scrape_and_email_batch_tennis_data_with_player_comparison(
    league_pool_combinations: List[Tuple[str, str]],
    recipients: List[str],
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    client_email: Optional[str] = None,
    include_google_sheets: bool = True,
    delay: float = None,
    batches: int = None,
):
    """Enhanced function that saves all players data to Google Sheets"""
    try:
        logger.info(f"Starting batch scrape and email workflow for {len(league_pool_combinations)} combinations")

        # Step 1: Scrape all the data for multiple combinations
        logger.info("Step 1: Starting batch data scraping...")
        batch_data = await collect_multiple_league_data(league_pool_combinations, delay, batches)

        if not batch_data or not batch_data.get('successful_combinations'):
            logger.error("No data was successfully scraped from any combination")
            return False

        logger.info("Batch data scraping completed successfully!")

        # Step 2: Save players data to Google Sheets (additional players sheets)
        players_sheet_url = None
        if include_google_sheets:
            if batch_data.get('players'):
                logger.info("Step 2: Saving players data to Google Sheets...")
                try:
                    # Create comparison result structure with players data
                    players_result = {
                        'players_data': batch_data.get('players', [])
                    }

                    players_sheet_url = await save_players_to_google_sheets(
                        players_result,
                        None,
                    )
                    if players_sheet_url:
                        logger.info('Google sheets for Players data created successfully')
                    else:
                        logger.warning("Failed to create Players Google Sheets document")
                except Exception as gs_error:
                    logger.warning(f"Google Sheets creation failed: {str(gs_error)}")

        # Step 3: Save main batch data to Excel
        logger.info("Step 3: Saving batch data to Excel...")
        excel_filename = await save_batch_to_excel(batch_data)

        if not excel_filename or not os.path.exists(excel_filename):
            logger.error("Failed to create Excel file")
            return False

        logger.info(f"Excel file created successfully: {excel_filename}")

        # Step 4: Save main batch data to Google Sheets
        google_sheets_url = None
        if include_google_sheets:
            logger.info("Step 4: Saving batch data to Google Sheets...")
            try:
                google_sheets_url = await save_batch_to_google_sheets(
                    batch_data,
                    None,
                )
                if google_sheets_url:
                    logger.info(f"Google Sheets created successfully: {google_sheets_url}")
                else:
                    logger.warning("Failed to create Google Sheets document")
            except Exception as gs_error:
                logger.warning(f"Google Sheets creation failed: {str(gs_error)}")

        # Step 5: Configure SMTP settings
        smtp_config = {
            'server': 'smtp.gmail.com',
            'port': 587,
            'username': EMAIL_USER,
            'password': EMAIL_PASS,
            'from_email': EMAIL_USER,
        }

        if not EMAIL_USER or not EMAIL_PASS:
            logger.error("Email credentials not found in environment variables")
            return False

        # Step 6: Send email with batch data and total players stats
        logger.info("Step 6: Sending email with batch data...")
        logger.info(f"About to send email with attachment: {excel_filename}")
        logger.info(f"File exists: {os.path.exists(excel_filename)}")
        logger.info(f"File size: {os.path.getsize(excel_filename) if os.path.exists(excel_filename) else 'N/A'} bytes")

        # Create simplified stats for email
        total_players = len(batch_data.get('players', []))
        players_stats = {
            'total_players': total_players
        }

        email_success = await send_batch_tennis_report_email_with_player_analysis(
            smtp_config=smtp_config,
            to_emails=recipients,
            excel_file_path=excel_filename,
            batch_summary=batch_data,
            comparison_result=players_stats,
            cc_emails=cc_emails,
            bcc_emails=bcc_emails,
            google_sheets_url=google_sheets_url,
            unmatched_players_sheet_url=players_sheet_url
        )

        if email_success:
            logger.info("Batch email sent successfully!")
            logger.info(f"Report sent to: {', '.join(recipients)}")
            if cc_emails:
                logger.info(f"CC: {', '.join(cc_emails)}")
            if bcc_emails:
                logger.info(f"BCC: {', '.join(bcc_emails)}")

            # Log summaries
            logger.info(f"Total combinations processed: {batch_data.get('total_processed', 0)}")
            logger.info(f"Successful: {len(batch_data.get('successful_combinations', []))}")
            logger.info(f"Failed: {len(batch_data.get('failed_combinations', []))}")
            logger.info(f"Total players processed: {total_players}")
        else:
            logger.error("Batch email sending failed!")

        return email_success

    except Exception as e:
        logger.error(f"Error in batch scrape and email workflow: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


async def send_batch_tennis_report_email_with_player_analysis(
    smtp_config: dict,
    to_emails: List[str],
    excel_file_path: str,
    batch_summary: dict,
    comparison_result: dict,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    google_sheets_url: Optional[str] = None,
    unmatched_players_sheet_url: Optional[str] = None
) -> bool:
    """Email template with total players statistics"""

    # Email subject
    timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H:%M:%S")
    subject = f"🎾 RankedIn League Data Report - {timestamp}"

    # HTML email body with total players stats
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #2c3e50;
                max-width: 650px;
                margin: 0 auto;
                padding: 0;
                background-color: #ffffff;
            }}
            .email-container {{
                background-color: #ffffff;
                border: 1px solid #e1e5e9;
                border-radius: 8px;
                overflow: hidden;
            }}
            .header-strip {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 25px 30px;
                color: white;
                text-align: center;
            }}
            .header-strip h1 {{
                margin: 0;
                font-size: 22px;
                font-weight: 600;
            }}
            .header-strip p {{
                margin: 5px 0 0 0;
                opacity: 0.9;
                font-size: 14px;
            }}
            .content-body {{
                padding: 30px;
            }}
            .greeting {{
                font-size: 16px;
                margin-bottom: 20px;
            }}
            .report-section {{
                background-color: #f8f9fa;
                border-radius: 6px;
                padding: 20px;
                margin: 25px 0;
                border-left: 4px solid #667eea;
            }}
            .report-title {{
                color: #495057;
                font-size: 18px;
                font-weight: 600;
                margin: 0 0 15px 0;
            }}
            .info-row {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 8px;
                padding: 5px 0;
                border-bottom: 1px solid #e9ecef;
            }}
            .info-row:last-child {{
                border-bottom: none;
            }}
            .info-label {{
                font-weight: 500;
                color: #6c757d;
            }}
            .info-value {{
                color: #495057;
                font-weight: 600;
            }}
            .metrics-container {{
                background-color: #ffffff;
                border: 1px solid #e9ecef;
                border-radius: 6px;
                padding: 20px;
                margin: 20px 0;
            }}
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
                gap: 15px;
                text-align: center;
            }}
            .metric-item {{
                padding: 12px;
                background-color: #f8f9fa;
                border-radius: 4px;
                border: 1px solid #e9ecef;
            }}
            .metric-number {{
                font-size: 20px;
                font-weight: 700;
                color: #667eea;
                display: block;
            }}
            .metric-label {{
                font-size: 11px;
                color: #6c757d;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-top: 4px;
            }}
            .additional-stats {{
                background-color: #e8f4fd;
                border-radius: 6px;
                padding: 18px;
                margin: 20px 0;
                border-left: 4px solid #17a2b8;
            }}
            .links-section {{
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 6px;
                margin: 20px 0;
                border: 1px solid #e9ecef;
            }}
            .links-section h4 {{
                color: #495057;
                margin: 0 0 10px 0;
                font-size: 16px;
            }}
            .link-item {{
                margin: 8px 0;
                padding: 8px 12px;
                background-color: #ffffff;
                border-radius: 4px;
                border: 1px solid #dee2e6;
            }}
            .link-item a {{
                color: #667eea;
                text-decoration: none;
                font-weight: 500;
            }}
            .link-item a:hover {{
                text-decoration: underline;
            }}
            .footer-section {{
                margin-top: 40px;
                padding-top: 20px;
                border-top: 2px solid #e9ecef;
                text-align: center;
            }}
            .signature-card {{
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 6px;
                text-align: left;
                display: inline-block;
                margin: 0 auto;
            }}
            .sig-name {{
                font-weight: 600;
                font-size: 16px;
                color: #2c3e50;
                margin-bottom: 4px;
            }}
            .sig-title {{
                color: #6c757d;
                font-size: 13px;
                margin-bottom: 12px;
            }}
            .sig-links {{
                font-size: 12px;
                line-height: 1.5;
            }}
            .sig-links a {{
                color: #667eea;
                text-decoration: none;
            }}
            .attachment-note {{
                background-color: #fff3cd;
                border: 1px solid #ffeaa7;
                padding: 12px;
                border-radius: 4px;
                margin: 15px 0;
                font-size: 14px;
                color: #856404;
            }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div class="header-strip">
                <h1>🎾 RankedIn League Data Report</h1>
                <p>Daily Automated RankedIn Stats</p>
            </div>

            <div class="content-body">
                <div class="greeting">
                    Hi {CLIENT_NAME},
                </div>

                <p>Here's your daily automated report containing comprehensive RankedIn league data and player statistics.</p>

                <div class="report-section">
                    <div class="report-title">League Data Summary</div>
                    <div class="info-row">
                        <span class="info-label">Report Generated: </span>
                        <span class="info-value">{datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%B %d, %Y at %I:%M %p')}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Excel File: </span>
                        <span class="info-value">{os.path.basename(excel_file_path)}</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">League Combinations: </span>
                        <span class="info-value">{batch_summary.get('total_processed', 0)}</span>
                    </div>
                </div>

                <div class="metrics-container">
                    <div class="metrics-grid">
                        <div class="metric-item">
                            <span class="metric-number">{len(batch_summary.get('standings', []))}</span>
                            <div class="metric-label">Standings</div>
                        </div>
                        <div class="metric-item">
                            <span class="metric-number">{len(batch_summary.get('rounds', []))}</span>
                            <div class="metric-label">Rounds</div>
                        </div>
                        <div class="metric-item">
                            <span class="metric-number">{len(batch_summary.get('matches', []))}</span>
                            <div class="metric-label">Matches</div>
                        </div>
                        <div class="metric-item">
                            <span class="metric-number">{len(batch_summary.get('organizations', []))}</span>
                            <div class="metric-label">Organizations</div>
                        </div>
                    </div>
                </div>

                <div class="additional-stats">
                    <div class="report-title">Total Players</div>
                    <div style="text-align: center; padding: 10px;">
                        <span style="font-size: 24px; font-weight: 700; color: #17a2b8;">{comparison_result.get('total_players', 0)}</span>
                        <div style="font-size: 12px; color: #6c757d; margin-top: 5px;">Players Processed</div>
                    </div>
                </div>

                {f'''
                <div class="links-section">
                    <h4>Access Your Data</h4>
                    {f'<div class="link-item">📊 <a href="{google_sheets_url}" target="_blank">RankedIn League Data Spreadsheet</a></div>' if google_sheets_url else ''}
                    {f'<div class="link-item">👥 <a href="{unmatched_players_sheet_url}" target="_blank">Players Data Spreadsheet</a></div>' if unmatched_players_sheet_url else ''}
                </div>
                ''' if google_sheets_url or unmatched_players_sheet_url else ''}

                <div class="attachment-note">
                    <strong>📎 Attachment:</strong> Complete RankedIn dataset is available in the attached Excel file.
                </div>

                <div class="footer-section">
                    <div class="signature-card">
                        <div class="sig-name">Sushil Bhandari</div>
                        <div class="sig-title">Python Developer & Automation Specialist</div>
                        <div class="sig-links">
                            📱 +977-9849892938<br>
                            🔗 <a href="https://www.upwork.com/freelancers/~017c0d983bfe5ba79f">Upwork Profile</a> |
                            <a href="https://www.linkedin.com/in/sushil-b-46594420a/">LinkedIn</a><br>
                            💻 <a href="https://github.com/sushil-rgb">GitHub</a> |
                            <a href="https://sushil-bhandari.com.np">Portfolio</a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    # Plain text version
    text_body = f"""
    🎾 RankedIn League Data Report - {timestamp}

    Hi {CLIENT_NAME},

    Here's your daily automated report containing comprehensive RankedIn league data and player statistics.

    LEAGUE DATA SUMMARY
    Report Generated: {datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%B %d, %Y at %I:%M %p')}
    Excel File: {os.path.basename(excel_file_path)}
    League Combinations: {batch_summary.get('total_processed', 0)}

    DATA METRICS
    Standings: {len(batch_summary.get('standings', []))}
    Rounds: {len(batch_summary.get('rounds', []))}
    Matches: {len(batch_summary.get('matches', []))}
    Organizations: {len(batch_summary.get('organizations', []))}

    PLAYER STATISTICS
    Total Players Processed: {comparison_result.get('total_players', 0)}

    ACCESS YOUR DATA
    {f'RankedIn League Data: {google_sheets_url}' if google_sheets_url else ''}
    {f'Players Data Spreadsheet: {unmatched_players_sheet_url}' if unmatched_players_sheet_url else ''}

    Attachment: Complete RankedIn dataset is available in the attached Excel file.

    --
    Sushil Bhandari
    Python Developer & Automation Specialist
    +977-9849892938
    https://www.upwork.com/freelancers/~017c0d983bfe5ba79f
    https://sushil-bhandari.com.np
    """

    return await send_email_with_attachment(
        smtp_server=smtp_config['server'],
        smtp_port=smtp_config['port'],
        smtp_username=smtp_config['username'],
        smtp_password=smtp_config['password'],
        from_email=smtp_config['from_email'],
        to_emails=to_emails,
        subject=subject,
        body=html_body,
        text_body=text_body,
        attachment_path=excel_file_path,
        cc_emails=cc_emails,
        bcc_emails=bcc_emails,
        is_html=True
    )

