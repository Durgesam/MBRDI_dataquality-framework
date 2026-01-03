import datetime
import logging
import smtplib
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pyspark.sql.functions as F
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from tabulate import tabulate


def send_email():
    """
    Sends test results as an email.
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        username = dbutils.secrets.get("CoCBigDataAI", "dqf-mail-user")
        password = dbutils.secrets.get("CoCBigDataAI", "dqf-mail-pw")
        server = dbutils.secrets.get(scope="CoCBigDataAI", key="dqf-mail-server")

        text = """
    Hello Team.

    This is to notify you that the Test cases Output
    {table}
    Regards,
    eXtollo Team"""

        html = """
    <html> 
                <head> 
                <style type="text/css">
            table {{
                font-family: CorpoS, sans-serif;
                width: 70%;
                }}
                .myTable {{
                width: 70%;
                text-align:center;
                background-color:#abcdd9;
                }}
                .myTable th {{
                background-color: #911f43;
                color: white;
                }}
                .myTable td, 
                .myTable th {{ 
                padding: 8px;
                border: 2px solid black; 
                }}
        </style>  
                </head> 
            <body> 
            <p>Hello Team,</p>
            <p>This is to notify you that below are the Test cases Output</p>
                <table class="myTable"> 
                <tr> 
                    <td>{table}</td> 
                </tr>
                </table>
                <p style="font-family:CorpoS">
                <br>Regards,<br>eXtollo Team
                </p> 

                </body> 
                </html>

        """

        data = spark.table("data_quality_framework")
        last_run_timestamp = data.select(F.max("load_ts")).collect()[0][0]
        last_run_data = data.filter(F.col("load_ts") == last_run_timestamp).toPandas()
        last_run_data = last_run_data[["test_case", "test_case_type", "test_status"]]
        text = text.format(
            table=tabulate(
                last_run_data,
                headers=["test_case", "test_case_type", "test_status"],
                showindex="False",
                tablefmt="grid",
            )
        )
        html = html.format(
            table=tabulate(
                last_run_data,
                headers=["test_case", "test_case_type", "test_status"],
                showindex="False",
                tablefmt="html",
            )
        )

        message = MIMEMultipart(
            "alternative", None, [MIMEText(text), MIMEText(html, "html")]
        )
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        message["Subject"] = f"Data Quality Framework Results {current_timestamp}"
        server = smtplib.SMTP(server)
        server.ehlo()
        server.starttls()
        server.login(username, password)
        sender = "no-reply@daimler.com"
        recipient = dbutils.secrets.get(scope="CoCBigDataAI", key="dqf-mail-recipients")
        server.sendmail(sender, recipient, message.as_string())
        server.close()
    except Exception:
        logging.info(
            "Credentials or IP Address which was passed are Failed & Didn't Sent Mail"
        )
        traceback.print_exc()
        print("Sending emailed failed")
