from datetime import datetime, timedelta, timezone
from db import Database

def run_seeder():
    db = Database()

    for t in ["users", "teams", "projects"]:
        try:
            db.col(t).delete_many({})
        except Exception:
            pass

    # USERS
    users = [
        {"name": "Alice", "email": "alice@example.com", "role": "manager"},
        {"name": "Bob", "email": "bob@example.com", "role": "dev"},
        {"name": "Charlie", "email": "charlie@example.com", "role": "dev"},
        {"name": "Diane", "email": "diane@example.com", "role": "designer"},
    ]
    user_pids = db.create_items("users", users, created_by="seeder")
    pid_by_email = {}
    for u, r in zip(users, user_pids):
        pid_by_email[u["email"]] = r["pid"]

    # TEAMS
    teams = [
        {"name": "Team A", "members": [pid_by_email["alice@example.com"], pid_by_email["bob@example.com"]]},
        {"name": "Team B", "members": [pid_by_email["charlie@example.com"], pid_by_email["diane@example.com"]]},
    ]
    team_pids = db.create_items("teams", teams, created_by="seeder")

    # PROJECTS
    now = datetime.now(timezone.utc)
    projects = [
        {"name": "Website Redesign", "teams": [team_pids[0]["pid"]], "tags": ["ui", "urgent"], "budget": 15000, "deadline": now + timedelta(days=30)},
        {"name": "Mobile App", "teams": [team_pids[1]["pid"]], "tags": ["mobile"], "budget": 40000, "deadline": now + timedelta(days=60)},
        {"name": "Data Pipeline", "teams": [team_pids[0]["pid"], team_pids[1]["pid"]], "tags": ["data", "urgent"], "budget": 55000, "deadline": now + timedelta(days=45)},
    ]
    db.create_items("projects", projects, created_by="seeder")

    print("✅ Seeder terminé.")

if __name__ == "__main__":
    run_seeder()
