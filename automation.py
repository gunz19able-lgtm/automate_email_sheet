from scraper import collect_multiple_league_data, save_batch_to_excel
from google_sheet_automation import save_batch_to_google_sheets
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from typing import List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from logger import setup_logger
from dotenv import load_dotenv
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

        # Handle attachment
        if attachment_path and os.path.exists(attachment_path):
            filename = os.path.basename(attachment_path)
            with open(attachment_path, "rb") as f:
                attachment_data = f.read()

            attachment_part = MIMEApplication(attachment_data, Name=filename)
            attachment_part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(attachment_part)


            # Determine MIME type
            if filename.endswith('.xlsx'):
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            elif filename.endswith('.xls'):
                mime_type = 'application/vnd.ms-excel'
            else:
                mime_type, _ = mimetypes.guess_type(attachment_path)
                if not mime_type:
                    mime_type = 'application/octet-stream'

            # Create attachment part
            attachment_part = MIMEApplication(attachment_data, Name=filename)
            attachment_part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(attachment_part)

            # Add headers
            attachment_part.add_header(
                'Content-Disposition',
                f'attachment; filename="{filename}"'
            )
            attachment_part.add_header('Content-Type', mime_type)
            attachment_part.add_header('Content-Transfer-Encoding', 'base64')

            msg.attach(attachment_part)

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

        print(f"✅ Email sent successfully to {len(recipients)} recipient(s)")
        return True

    except Exception as e:
        print(f"❌ Failed to send email: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False


async def send_batch_tennis_report_email(
    smtp_config: dict,
    to_emails: List[str],
    excel_file_path: str,
    batch_summary: dict,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None
) -> bool:

    # Email subject
    timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H:%M:%S")
    subject = f"🎾 RankedIn League Data Report - {timestamp}"

    # HTML email body with professional styling
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f8f9fa;
            }}
            .container {{
                background-color: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            .header {{
                border-bottom: 3px solid #28a745;
                padding-bottom: 15px;
                margin-bottom: 25px;
            }}
            .header h2 {{
                color: #28a745;
                margin: 0;
                font-size: 24px;
            }}
            .summary-section {{
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
                border-left: 4px solid #007bff;
            }}
            .data-summary {{
                background-color: #e9ecef;
                padding: 15px;
                border-radius: 6px;
                margin: 20px 0;
                font-family: monospace;
                text-align: center;
            }}
            .signature {{
                margin-top: 30px;
                padding-top: 20px;
                border-top: 2px solid #e9ecef;
            }}
            .signature-content {{
                display: flex;
                align-items: flex-start;
                gap: 15px;
            }}
            .signature-text {{
                flex: 1;
            }}
            .signature-name {{
                font-weight: bold;
                font-size: 16px;
                color: #2c3e50;
                margin-bottom: 5px;
            }}
            .signature-title {{
                color: #7f8c8d;
                font-size: 14px;
                margin-bottom: 10px;
            }}
            .signature-contact {{
                font-size: 12px;
                line-height: 1.4;
            }}
            .signature-contact a {{
                color: #3498db;
                text-decoration: none;
            }}
            .signature-contact a:hover {{
                text-decoration: underline;
            }}
            .highlight {{
                background-color: #fff3cd;
                padding: 2px 6px;
                border-radius: 4px;
                font-weight: 500;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>🎾 RankedIn League Data Report</h2>
                <p style="margin: 5px 0; color: #6c757d;">Comprehensive Tennis League Analytics</p>
            </div>

            <p>Hi {CLIENT_NAME},</p>

            <p>Please find attached the comprehensive league data report for your review. This automated report contains the latest league statistics.</p>

            <div class="summary-section">
                <h3 style="margin-top: 0; color: #495057;">📊 Report Summary</h3>
                <p><strong>Generated:</strong> {datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%A, %B %d, %Y at %I:%M %p')}</p>
                <p><strong>File:</strong> <span class="highlight">{os.path.basename(excel_file_path)}</span></p>
                <p><strong>Total League/Pool Combinations:</strong> {batch_summary.get('total_processed', 0)}</p>
            </div>

            <h3 style="color: #495057;">📈 Data Summary</h3>
            <div class="data-summary">
                <strong>Standings:</strong> {len(batch_summary.get('standings', []))} |
                <strong>Rounds:</strong> {len(batch_summary.get('rounds', []))} |
                <strong>Players:</strong> {len(batch_summary.get('players', []))} |
                <strong>Matches:</strong> {len(batch_summary.get('matches', []))} |
                <strong>Organizations:</strong> {len(batch_summary.get('organizations', []))}
            </div>

            <div class="signature">
                <div class="signature-content">
                    <div class="signature-text">
                        <div class="signature-name">Sushil Bhandari</div>
                        <div class="signature-title">Python Developer | Web Scraping & Automation Specialist</div>
                        <div class="signature-contact">
                            📱 <a href="tel:+977-9849892938">+977-9849892938</a><br>
                            🔗 <a href="https://www.upwork.com/freelancers/~017c0d983bfe5ba79f" target="_blank">Upwork</a> |
                            <a href="https://www.linkedin.com/in/sushil-b-46594420a//" target="_blank">LinkedIn</a> |
                            <a href="https://github.com/sushil-rgb" target="_blank">GitHub</a> |
                            <a href="https://sushil-bhandari.com.np" target="_blank">Website</a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    # Plain text version for email clients that don't support HTML
    text_body = f"""
    RankedIn League Data Report - {timestamp}

    Hi {CLIENT_NAME},

    Please find attached the comprehensive league data report.

    Report Details:
    • Generated: {datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%Y-%m-%d %H:%M:%S')}
    • File: {os.path.basename(excel_file_path)}
    • Total League/Pool Combinations: {batch_summary.get('total_processed', 0)}

    Data Summary: Standings: {len(batch_summary.get('standings', []))} | Rounds: {len(batch_summary.get('rounds', []))} | Players: {len(batch_summary.get('players', []))} | Matches: {len(batch_summary.get('matches', []))} | Organizations: {len(batch_summary.get('organizations', []))}

    Best regards,

    Sushil Bhandari
    Python Developer | Web Scraping & Automation Specialist
    Phone: +977-9849892938
    Upwork: https://www.upwork.com/freelancers/sushilbhandari
    LinkedIn: https://www.linkedin.com/in/sushil-bhandari/
    GitHub: https://github.com/yourusername
    Website: https://sushil-bhandari.com.np
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


async def scrape_and_email_batch_tennis_data(
    league_pool_combinations: List[Tuple[str, str]],
    recipients: List[str],
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    client_email: Optional[str] = None,
    include_google_sheets: bool = True,
    delay: float = None,
    batches: int = None,
):

    try:
        logger.info(f"Starting batch scrape and email workflow for {len(league_pool_combinations)} combinations")

        # Step 1: Scrape all the data for multiple combinations (ONLY ONCE)
        logger.info("Step 1: Starting batch data scraping...")
        batch_data = await collect_multiple_league_data(league_pool_combinations, delay, batches)

        if not batch_data or not batch_data.get('successful_combinations'):
            logger.error("No data was successfully scraped from any combination")
            return False

        logger.info("Batch data scraping completed successfully!")

        # Step 2: Save to Excel
        logger.info("Step 2: Saving batch data to Excel...")
        excel_filename = await save_batch_to_excel(batch_data)

        if not excel_filename or not os.path.exists(excel_filename):
            logger.error("Failed to create Excel file")
            return False

        logger.info(f"Excel file created successfully: {excel_filename}")

        # Step 3: Save to Google Sheets
        google_sheets_url = None
        if include_google_sheets:
            logger.info("Step 3: Saving batch data to Google Sheets...")
            try:
                google_sheets_url = await save_batch_to_google_sheets(batch_data, client_email)
                if google_sheets_url:
                    logger.info(f"Google Sheets created successfully: {google_sheets_url}")
                    if client_email:
                        logger.info(f"Google Sheets shared with: {client_email}")
                else:
                    logger.warning("Failed to create Google Sheets document")
            except Exception as gs_error:
                logger.warning(f"Google Sheets creation failed: {str(gs_error)}")
                logger.info("Continuing with Excel file only...")

        # Step 4: Configure SMTP settings
        smtp_config = {
            'server': 'smtp.gmail.com',
            'port': 587,
            'username': EMAIL_USER,
            'password': EMAIL_PASS,
            'from_email': EMAIL_USER,
        }

        # Validate email configuration
        if not EMAIL_USER or not EMAIL_PASS:
            logger.error("Email credentials not found in environment variables")
            return False

        # Step 5: Send the email with attachment and Google Sheets link
        logger.info("Step 4: Sending email with batch data...")
        email_success = await send_batch_tennis_report_email(
            smtp_config=smtp_config,
            to_emails=recipients,
            excel_file_path=excel_filename,
            batch_summary=batch_data,
            cc_emails=cc_emails,
            bcc_emails=bcc_emails,
            google_sheets_url=google_sheets_url
        )

        if email_success:
            logger.info("Batch email sent successfully!")
            logger.info(f"Report sent to: {', '.join(recipients)}")
            if cc_emails:
                logger.info(f"CC: {', '.join(cc_emails)}")
            if bcc_emails:
                logger.info(f"BCC: {', '.join(bcc_emails)}")
            if google_sheets_url:
                logger.info(f"Google Sheets URL included in email: {google_sheets_url}")

            # Log summary
            logger.info(f"Total combinations processed: {batch_data.get('total_processed', 0)}")
            logger.info(f"Successful: {len(batch_data.get('successful_combinations', []))}")
            logger.info(f"Failed: {len(batch_data.get('failed_combinations', []))}")

        else:
            logger.error("Batch email sending failed!")

        return email_success

    except Exception as e:
        logger.error(f"Error in batch scrape and email workflow: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


# Updated email sending function to include Google Sheets URL
async def send_batch_tennis_report_email(
    smtp_config: dict,
    to_emails: List[str],
    excel_file_path: str,
    batch_summary: dict,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    google_sheets_url: Optional[str] = None  # New parameter
):

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = smtp_config['from_email']
        msg['To'] = ', '.join(to_emails)

        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)

        # Get report details for subject
        timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H:%M:%S")
        subject = f"🎾 RankedIn League Data Report - {timestamp}"
        msg['Subject'] = subject

        # HTML email body with professional styling - as a STRING, not list
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f8f9fa;
                }}
                .container {{
                    background-color: white;
                    padding: 30px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .header {{
                    border-bottom: 3px solid #28a745;
                    padding-bottom: 15px;
                    margin-bottom: 25px;
                }}
                .header h2 {{
                    color: #28a745;
                    margin: 0;
                    font-size: 24px;
                }}
                .summary-section {{
                    background-color: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    margin: 20px 0;
                    border-left: 4px solid #007bff;
                }}
                .data-summary {{
                    background-color: #e9ecef;
                    padding: 15px;
                    border-radius: 6px;
                    margin: 20px 0;
                    font-family: monospace;
                    text-align: center;
                }}
                .signature {{
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 2px solid #e9ecef;
                }}
                .signature-content {{
                    display: flex;
                    align-items: flex-start;
                    gap: 15px;
                }}
                .signature-text {{
                    flex: 1;
                }}
                .signature-name {{
                    font-weight: bold;
                    font-size: 16px;
                    color: #2c3e50;
                    margin-bottom: 5px;
                }}
                .signature-title {{
                    color: #7f8c8d;
                    font-size: 14px;
                    margin-bottom: 10px;
                }}
                .signature-contact {{
                    font-size: 12px;
                    line-height: 1.4;
                }}
                .signature-contact a {{
                    color: #3498db;
                    text-decoration: none;
                }}
                .signature-contact a:hover {{
                    text-decoration: underline;
                }}
                .highlight {{
                    background-color: #fff3cd;
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-weight: 500;
                }}
                .google-sheets {{
                    background-color: #d1ecf1;
                    padding: 15px;
                    border-radius: 6px;
                    margin: 20px 0;
                    border-left: 4px solid #0dcaf0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>🎾 RankedIn League Data Report</h2>
                    <p style="margin: 5px 0; color: #6c757d;">Comprehensive Tennis League Analytics</p>
                </div>

                <p>Hi {CLIENT_NAME},</p>

                <p>Here’s your daily automated report with the latest league statistics.</p>

                <div class="summary-section">
                    <h3 style="margin-top: 0; color: #495057;">📊 Report Summary</h3>
                    <p><strong>Generated:</strong> {datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%A, %B %d, %Y at %I:%M %p')}</p>
                    <p><strong>File:</strong> <span class="highlight">{os.path.basename(excel_file_path)}</span></p>
                    <p><strong>Total League/Pool Combinations:</strong> {batch_summary.get('total_processed', 0)}</p>
                </div>

                <h3 style="color: #495057;">📈 Data Summary</h3>
                <div class="data-summary">
                    <strong>Standings:</strong> {len(batch_summary.get('standings', []))} |
                    <strong>Rounds:</strong> {len(batch_summary.get('rounds', []))} |
                    <strong>Players:</strong> {len(batch_summary.get('players', []))} |
                    <strong>Matches:</strong> {len(batch_summary.get('matches', []))} |
                    <strong>Organizations:</strong> {len(batch_summary.get('organizations', []))}
                </div>

                {f'''
                <div class="google-sheets">
                    <h4 style="margin-top: 0; color: #0a58ca;">🔗 Live Google Sheets Access</h4>
                    <p><strong>Link:</strong> <a href="{google_sheets_url}" target="_blank" style="color: #0a58ca;">{google_sheets_url}</a></p>
                    <p style="margin-bottom: 0;"><strong>💡 Note:</strong> The Google Sheets version is live, but it cannot be accessed by everyone. Only people who have been given access can view, edit, and collaborate on the data in real-time.</p>
                </div>
                ''' if google_sheets_url else ''}

                <div class="signature">
                    <div class="signature-content">
                        <div class="signature-text">
                            <div class="signature-name">Sushil Bhandari</div>
                            <div class="signature-title">Python Developer | Web Scraping & Automation Specialist</div>
                            <div class="signature-contact">
                                📱 <a href="tel:+977-9849892938">+977-9849892938</a><br>
                                🔗 <a href="https://www.upwork.com/freelancers/~017c0d983bfe5ba79f" target="_blank">Upwork</a> |
                                <a href="https://www.linkedin.com/in/sushil-b-46594420a//" target="_blank">LinkedIn</a> |
                                <a href="https://github.com/sushil-rgb" target="_blank">GitHub</a> |
                                <a href="https://sushil-bhandari.com.np" target="_blank">Website</a>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """

        text_lines = [
            f"RankedIn League Data Report - {timestamp}",
            "",
            f"Hi {CLIENT_NAME},",
            "",
            "Here’s your daily automated report with the latest league statistics.",
            "",
            "Report Details:",
            f"• Generated: {datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%Y-%m-%d %H:%M:%S')}",
            f"• File: {os.path.basename(excel_file_path)}",
            f"• Total League/Pool Combinations: {batch_summary.get('total_processed', 0)}",
            "",
            f"Data Summary: Standings: {len(batch_summary.get('standings', []))} | Rounds: {len(batch_summary.get('rounds', []))} | Players: {len(batch_summary.get('players', []))} | Matches: {len(batch_summary.get('matches', []))} | Organizations: {len(batch_summary.get('organizations', []))}",
            ""
        ]

        if google_sheets_url:
            text_lines.extend([
                "🔗 Live Google Sheets Access:",
                f"• Link: {google_sheets_url}",
                "",
                "💡 Note: The Google Sheets version is live, but it cannot be accessed by everyone. Only people who have been given access can view, edit, and collaborate on the data in real-time.",
                ""
            ])

        text_lines.extend([
            f"Report generated on: {datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%Y-%m-%d at %H:%M:%S')}",
            "",
            "Best regards,",
            "",
            "Sushil Bhandari",
            "Python Developer | Web Scraping & Automation Specialist",
            "Phone: +977-9849892938",
            "Upwork: https://www.upwork.com/freelancers/~017c0d983bfe5ba79f",
            "LinkedIn: https://www.linkedin.com/in/sushil-b-46594420a//",
            "GitHub: https://github.com/sushil-rgb",
            "Website: https://sushil-bhandari.com.np"
        ])

        text_body = '\n'.join(text_lines)

        msg.attach(MIMEText(html_body, 'html'))

        if os.path.exists(excel_file_path):
            with open(excel_file_path, "rb") as attachment:
                part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            filename = os.path.basename(excel_file_path)

            part.add_header(
                'Content-Disposition',
                'attachment',
                filename=filename
            )
            msg.attach(part)
        else:
            logger.error(f"Excel file not found: {excel_file_path}")
            return False

        # Connect to server and send email
        server = smtplib.SMTP(smtp_config['server'], smtp_config['port'])
        server.starttls()
        server.login(smtp_config['username'], smtp_config['password'])

        all_recipients = to_emails[:]
        if cc_emails:
            all_recipients.extend(cc_emails)
        if bcc_emails:
            all_recipients.extend(bcc_emails)

        text = msg.as_string()
        server.sendmail(smtp_config['from_email'], all_recipients, text)
        server.quit()

        logger.info("Email sent successfully!")
        return True

    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return False

