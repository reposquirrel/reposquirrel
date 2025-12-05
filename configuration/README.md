# Configuration Files

This folder contains all configuration files for the repository analytics application.

## Files

### `services.json`
Defines how repositories are organized into services/subsystems. Maps repository names to their internal services and the paths that belong to each service.

**Format:**
```json
{
  "repository/name": {
    "service-name": ["path1/", "path2/"],
    "other-service": ["path3/"]
  }
}
```

### `teams.json`
Defines development teams and their members.

**Format:**
```json
{
  "team-id": {
    "name": "Team Display Name",
    "description": "Team description",
    "members": ["user1", "user2", "user3"]
  }
}
```

### `alias.json`
Maps canonical usernames to alternative user identifiers. Used to consolidate statistics for users with multiple accounts.

**Format:**
```json
{
  "canonical-username": ["alias1", "alias2", "alias3"],
  "other-user": ["alternative-name"]
}
```

### `team_subsystem_responsibilities.json`
Maps teams to the subsystems they are responsible for. Used for ownership tracking and responsibility assignment.

**Format:**
```json
{
  "team-id": ["subsystem1", "subsystem2"],
  "other-team": ["subsystem3"]
}
```

### `ignore_user.txt`
Lists users that should be excluded from statistics (one per line). Typically includes bots, automated accounts, or other non-human contributors.

**Format:**
```
user-to-ignore-1
user-to-ignore-2
bot-account
```

## Usage

These configuration files are automatically loaded by the application and can be edited through the web interface (Settings → respective tab) or by directly editing the files.

**Important:** After making changes to these files, the application may need to be restarted or data may need to be regenerated depending on the type of change.

## File Descriptions

- **services.json**: Controls how repositories are broken down into smaller components/services for analysis
- **teams.json**: Organizes developers into teams for team-based statistics and reporting
- **alias.json**: Helps merge statistics for developers who have multiple Git identities
- **team_subsystem_responsibilities.json**: Links teams to the subsystems they own/maintain
- **ignore_user.txt**: Excludes automated accounts from developer statistics

All files use standard JSON format except `ignore_user.txt` which is a simple text file with one username per line.