#!/usr/bin/env bash
set -euo pipefail

readonly example_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${example_directory}/compose.yaml"
readonly project_name="catga-distributed-todo-verify-${RANDOM}"

cleanup() {
  docker compose --project-name "${project_name}" --file "${compose_file}" down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose --project-name "${project_name}" --file "${compose_file}" up --build --detach

for attempt in {1..60}; do
  if curl --fail --silent --show-error http://127.0.0.1:3000/healthz >/dev/null; then
    break
  fi
  if [[ "${attempt}" == "60" ]]; then
    docker compose --project-name "${project_name}" --file "${compose_file}" logs >&2
    exit 1
  fi
  sleep 1
done

if ! docker compose --project-name "${project_name}" --file "${compose_file}" logs worker 2>&1 |
  grep --fixed-strings --quiet "distributed Todo worker is consuming Todo commands"; then
  docker compose --project-name "${project_name}" --file "${compose_file}" logs >&2
  exit 1
fi

title="deliver the quarterly report"
curl --fail --silent --show-error \
  --header "content-type: application/json" \
  --data "{\"title\":\"${title}\"}" \
  http://127.0.0.1:3000/todos >/dev/null

for attempt in {1..60}; do
  todos="$(curl --fail --silent --show-error http://127.0.0.1:3000/todos)"
  if grep --fixed-strings --quiet "${title}" <<<"${todos}"; then
    break
  fi
  if [[ "${attempt}" == "60" ]]; then
    docker compose --project-name "${project_name}" --file "${compose_file}" logs >&2
    exit 1
  fi
  sleep 1
done

docker compose --project-name "${project_name}" --file "${compose_file}" restart api >/dev/null
for attempt in {1..60}; do
  if curl --fail --silent --show-error http://127.0.0.1:3000/healthz >/dev/null; then
    break
  fi
  if [[ "${attempt}" == "60" ]]; then
    docker compose --project-name "${project_name}" --file "${compose_file}" logs >&2
    exit 1
  fi
  sleep 1
done

restarted_todos="$(curl --fail --silent --show-error http://127.0.0.1:3000/todos)"
if ! grep --fixed-strings --quiet "${title}" <<<"${restarted_todos}"; then
  docker compose --project-name "${project_name}" --file "${compose_file}" logs >&2
  exit 1
fi

printf 'distributed Todo restart verification passed\n'

exit 0
