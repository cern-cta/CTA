# Load GitLab plugin
warn("danger-gitlab plugin not loaded") unless defined? danger.gitlab

# --- Constants ---

ALLOWED_PREFIXES = %w[
  [CI] [Misc] [Tools] [catalogue]
  [frontend] [scheduler] [taped] [rmcd]
]

REVIEWERS = %w[alice bob carol] # Replace with actual usernames

# --- Title checks ---

unless ALLOWED_PREFIXES.any? { |prefix| gitlab.mr_title.start_with?(prefix) }
  fail("MR title must start with one of: #{ALLOWED_PREFIXES.join(', ')}")
end

warn("MR title is long. Consider shortening it to less than 72 characters") if gitlab.mr_title.length > 72

# --- Description checks ---

desc = gitlab.mr_body.strip

if desc.empty? || desc.include?("<!-- Mandatory: provide a description")
  fail("MR description is missing or incomplete (please follow the template)")
end

# --- Label checks ---

labels = gitlab.mr_json["labels"] || []

type_labels = labels.select { |l| l.start_with?("type::") }
workflow_labels = labels.select { |l| l.start_with?("workflow::") }
other_labels = labels - (type_labels + workflow_labels)

fail("MR must include exactly one 'type::' label") if type_labels.size != 1
fail("MR must include exactly one 'workflow::' label") if workflow_labels.size != 1
warn("MR only contains type:: and workflow:: labels. Consider adding additional labels to define the scope") if other_labels.empty?

# --- Assignee check ---

if gitlab.mr_json["assignee"].nil?
  warn("No assignee set — assigning author")

  gitlab.api.put(
    "/projects/#{gitlab.mr_json['project_id']}/merge_requests/#{gitlab.mr_json['iid']}",
    assignee_id: gitlab.mr_json["author"]["id"]
  )
end

# --- Reviewer assignment (round-robin) ---

if !gitlab.mr_draft && (gitlab.mr_json["reviewers"].nil? || gitlab.mr_json["reviewers"].empty?)
  warn("No reviewer is assigned to this merge request.")
end


# --- MR size warnings ---

added = git.lines_of_code[:added]
deleted = git.lines_of_code[:deleted]
files_changed = (git.modified_files + git.added_files + git.deleted_files).uniq.size

warn("MR is large: #{added + deleted} lines changed") if (added + deleted) > 500
warn("MR touches many files: #{files_changed} files") if files_changed > 20
