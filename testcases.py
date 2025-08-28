import pandas as pd

# Define the columns
columns = ["Test Case ID", "Pre-condition", "Summary", "Steps to Execute", "Expected Result", "Flow"]

# All test cases as list of dictionaries
test_cases = [
    # Delivery Modes
    {"Test Case ID": "TC-DM-001", "Pre-condition": "User is on Delivery Modes section", "Summary": "Verify In-App toggle is selectable", "Steps to Execute": "1. Click 'In-App' delivery card", "Expected Result": "Checkbox/icon updates to selected state", "Flow": "Positive"},
    {"Test Case ID": "TC-DM-002", "Pre-condition": "Delivery Modes section visible", "Summary": "Verify Email toggle is selectable", "Steps to Execute": "1. Click 'Email' toggle", "Expected Result": "Checkbox icon updates to checked state", "Flow": "Positive"},
    {"Test Case ID": "TC-DM-003", "Pre-condition": "Delivery Modes section visible", "Summary": "Verify SMS toggle is selectable", "Steps to Execute": "1. Click 'SMS' toggle", "Expected Result": "Checkbox icon updates to checked state", "Flow": "Positive"},
    {"Test Case ID": "TC-DM-004", "Pre-condition": "Email toggle pre-selected", "Summary": "Verify unchecking Email", "Steps to Execute": "1. Click Email toggle again", "Expected Result": "Checkbox icon updates to unchecked state", "Flow": "Positive"},
    {"Test Case ID": "TC-DM-005", "Pre-condition": "User tries to select multiple modes", "Summary": "Verify multiple selection", "Steps to Execute": "1. Select In-App\n2. Select Email\n3. Select SMS", "Expected Result": "All selected modes remain checked", "Flow": "Positive"},
    {"Test Case ID": "TC-DM-006", "Pre-condition": "Delivery Modes section loaded", "Summary": "Verify toggle persistence", "Steps to Execute": "1. Select modes\n2. Refresh page", "Expected Result": "Selected modes remain checked", "Flow": "Positive"},
    {"Test Case ID": "TC-DM-007", "Pre-condition": "Network error", "Summary": "Verify toggle behavior on API failure", "Steps to Execute": "1. Click toggle while API offline", "Expected Result": "Shows error or reverts toggle", "Flow": "Negative"},
    {"Test Case ID": "TC-DM-008", "Pre-condition": "Invalid state", "Summary": "Verify UI with disabled delivery mode", "Steps to Execute": "1. Disable 'In-App' in config\n2. Try selecting", "Expected Result": "Toggle cannot be selected", "Flow": "Negative"},

    # SAS-Alerts
    {"Test Case ID": "TC-SAS-001", "Pre-condition": "SAS-Alerts visible", "Summary": "Verify All Off toggle", "Steps to Execute": "1. Click 'All Off' toggle", "Expected Result": "All alert toggles move to off position", "Flow": "Positive"},
    {"Test Case ID": "TC-SAS-002", "Pre-condition": "SAS-Alerts visible", "Summary": "Verify individual alert toggle", "Steps to Execute": "1. Click Received alert toggle", "Expected Result": "Toggle switches to on/off", "Flow": "Positive"},
    {"Test Case ID": "TC-SAS-003", "Pre-condition": "SAS-Alerts visible", "Summary": "Verify multiple alert toggles", "Steps to Execute": "1. Switch Received and Shipped toggles", "Expected Result": "Both toggles reflect selected state", "Flow": "Positive"},
    {"Test Case ID": "TC-SAS-004", "Pre-condition": "User toggles alerts", "Summary": "Verify persistence", "Steps to Execute": "1. Select alerts\n2. Refresh page", "Expected Result": "Selected alerts remain", "Flow": "Positive"},
    {"Test Case ID": "TC-SAS-005", "Pre-condition": "API offline", "Summary": "Verify error handling", "Steps to Execute": "1. Toggle alerts with API down", "Expected Result": "Shows error or reverts state", "Flow": "Negative"},
    {"Test Case ID": "TC-SAS-006", "Pre-condition": "Edge case", "Summary": "Rapid toggling", "Steps to Execute": "1. Click toggle rapidly multiple times", "Expected Result": "No crash, toggles function normally", "Flow": "Edge"},
    {"Test Case ID": "TC-SAS-007", "Pre-condition": "Accessibility", "Summary": "Verify keyboard navigation", "Steps to Execute": "1. Tab to toggle\n2. Press Spacebar", "Expected Result": "Toggle state changes", "Flow": "Positive"},

    # Custom Trackers Alerts
    {"Test Case ID": "TC-CT-001", "Pre-condition": "Custom Trackers visible", "Summary": "Verify Active only toggle", "Steps to Execute": "1. Click 'Active only' toggle", "Expected Result": "Only active tracker alerts are shown", "Flow": "Positive"},
    {"Test Case ID": "TC-CT-002", "Pre-condition": "Custom Trackers visible", "Summary": "Verify All On/All Off toggle", "Steps to Execute": "1. Click All On/All Off toggle", "Expected Result": "All tracker alerts switch accordingly", "Flow": "Positive"},
    {"Test Case ID": "TC-CT-003", "Pre-condition": "Custom Trackers visible", "Summary": "Verify individual alert toggle", "Steps to Execute": "1. Click single tracker alert toggle", "Expected Result": "Single alert switches on/off", "Flow": "Positive"},
    {"Test Case ID": "TC-CT-004", "Pre-condition": "API dependent", "Summary": "Verify behavior on API failure", "Steps to Execute": "1. Toggle alert while API offline", "Expected Result": "Shows error or no change", "Flow": "Negative"},
    {"Test Case ID": "TC-CT-005", "Pre-condition": "Large number of trackers", "Summary": "Verify performance", "Steps to Execute": "1. Load page with 50+ trackers", "Expected Result": "Page loads smoothly, toggles functional", "Flow": "Edge"},
    {"Test Case ID": "TC-CT-006", "Pre-condition": "Rapid toggling", "Summary": "Verify stability", "Steps to Execute": "1. Click toggle rapidly", "Expected Result": "No crash, UI remains consistent", "Flow": "Edge"},
    {"Test Case ID": "TC-CT-007", "Pre-condition": "Accessibility", "Summary": "Keyboard navigation", "Steps to Execute": "1. Tab to toggle\n2. Press Spacebar", "Expected Result": "Toggle state changes", "Flow": "Positive"},

    # Dropdown & Page-level
    {"Test Case ID": "TC-DROP-001", "Pre-condition": "Dropdown visible", "Summary": "Verify dropdown opens on click", "Steps to Execute": "1. Click dropdown", "Expected Result": "Dropdown expands showing available options", "Flow": "Positive"},
    {"Test Case ID": "TC-DROP-002", "Pre-condition": "Dropdown has options", "Summary": "Verify option selection updates value", "Steps to Execute": "1. Select valid option", "Expected Result": "Selected option appears and updates form data", "Flow": "Positive"},
    {"Test Case ID": "TC-GEN-001", "Pre-condition": "Page loaded", "Summary": "Verify page renders correctly", "Steps to Execute": "1. Open page", "Expected Result": "All sections visible and scrollable", "Flow": "Positive"},
    {"Test Case ID": "TC-GEN-002", "Pre-condition": "Page loaded", "Summary": "Verify scroll functionality", "Steps to Execute": "1. Scroll main container", "Expected Result": "All sections accessible", "Flow": "Positive"}
]

# Create DataFrame
df = pd.DataFrame(test_cases, columns=columns)

# Save to Excel
df.to_excel('/mnt/data/Shield_Alert_System_Test_Cases.xlsx', index=False)

"Shield Alert System test cases Excel created successfully."
