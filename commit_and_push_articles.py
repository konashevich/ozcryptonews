import subprocess
import sys

def run_command(command):
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(result.returncode)
    else:
        print(result.stdout)

def main():
    # Add articles.csv to the staging area
    run_command(["git", "add", "articles.csv"])
    # Commit the changes with a message
    run_command(["git", "commit", "-m", "Update articles.csv"])
    # Push the commit to the repository's main branch
    run_command(["git", "push", "https://github.com/konashevich/ozcryptonews.git", "HEAD:main"])

if __name__ == "__main__":
    main()