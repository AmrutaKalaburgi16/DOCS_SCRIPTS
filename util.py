import os
import subprocess

def send_email_notification(obj, script_file=__file__, to_email="Amruta.Kalaburgi@lumen.com"):
    """
    Generic email notification utility. Accepts a dictionary and sends all key-value pairs in the body.
    """
    Subject = f"STATUS: {os.path.basename(script_file)}"
    Body = ""
    for k, v in obj.items():
        Body += f"{k}: {v}\n"
    body_str_encoded_to_byte = Body.encode()
    subprocess.run(
        ["mail", "-s", Subject, to_email],
        input=body_str_encoded_to_byte,
    )

