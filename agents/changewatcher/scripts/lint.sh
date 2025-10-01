#!/usr/bin/env sh
set -eu

: "${COMPONENT_NAME:=agent-changewatcher}"
: "${ARTIFACTS_DIR:=/artifacts}"
: "${CACHE_DIR:=/cache}"

mkdir -p "${ARTIFACTS_DIR}" "${CACHE_DIR}"

case "$(basename "$0")" in
  lint.sh)
    echo "Running lint for ${COMPONENT_NAME} (skeleton)."
    printf 'lint_ok=true\ncomponent=%s\n' "${COMPONENT_NAME}" \
      > "${ARTIFACTS_DIR}/${COMPONENT_NAME}_lint.log"
    ;;
  test.sh)
    echo "Executing placeholder unit tests for ${COMPONENT_NAME}."
    cat <<XML > "${ARTIFACTS_DIR}/${COMPONENT_NAME}_tests.xml"
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="${COMPONENT_NAME}" tests="1" failures="0">
  <testcase classname="placeholder" name="succeeds"/>
</testsuite>
XML
    ;;
  build.sh)
    artifact="${ARTIFACTS_DIR}/${COMPONENT_NAME}_build.txt"
    echo "Building ${COMPONENT_NAME} (placeholder)." | tee "${artifact}"
    printf 'artifact=%s\n' "${artifact}" >> "${artifact}"
    ;;
  publish.sh)
    artifact="${ARTIFACTS_DIR}/${COMPONENT_NAME}_publish.log"
    {
      echo "Publishing ${COMPONENT_NAME} (placeholder)."
      echo "timestamp=$(date -u +%FT%TZ)"
    } > "${artifact}"
    ;;
  *)
    echo "Unknown script $(basename "$0")" >&2
    exit 1
    ;;
esac
