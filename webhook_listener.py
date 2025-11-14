import os
import subprocess
from flask import Flask, request, abort

app = Flask(__name__)

# Set your GitHub secret token for verification (optional, but recommended)
GITHUB_SECRET = "your_github_secret_token_here"  # Replace with your secret token

# Define your project directory and virtual environment
PROJECT_DIR = '/home/ubuntu/aws-ff-data'
VENV_DIR = '/home/ubuntu/aws-ff-data/aws-ff-data-env'

# Define the Streamlit entry file
STREAMLIT_FILE = '/home/ubuntu/aws-ff-data/0_Home.py'


def update_and_restart_streamlit():
    """
    Pulls the latest changes from the repository, installs dependencies,
    and restarts the Streamlit app.
    """
    # Activate virtual environment
    activate_venv_command = f"source {VENV_DIR}/bin/activate"
    
    # Pull the latest changes from GitHub
    pull_command = f"git -C {PROJECT_DIR} pull origin main"
    
    # Restart Streamlit app
    restart_command = f"screen -S streamlit-app -X quit && screen -S streamlit-app bash -c 'source {VENV_DIR}/bin/activate && streamlit run {STREAMLIT_FILE} --server.port 8501'"

    # Execute commands
    subprocess.run(activate_venv_command, shell=True, executable="/bin/bash")
    subprocess.run(pull_command, shell=True, executable="/bin/bash")
    subprocess.run(restart_command, shell=True, executable="/bin/bash")


@app.route('/webhook', methods=['POST'])
def github_webhook():
    """
    Receives the webhook payload from GitHub and processes it.
    """
    if request.method == 'POST':
        # Verify the GitHub secret token
        if request.headers.get('X-Hub-Signature') != GITHUB_SECRET:
            abort(403)

        # If the event is a push to the main branch, update and restart the server
        payload = request.json
        if payload['ref'] == 'refs/heads/main':
            update_and_restart_streamlit()
            return "Streamlit app updated and restarted!", 200
        else:
            return "No updates to main branch.", 200

    else:
        abort(400)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)

