document.addEventListener('DOMContentLoaded', (event) => {
    document.getElementById("filterBy").addEventListener("submit", function (e) {
        // When filter button is pressed gets filtered records

        e.preventDefault(); // Prevent the default form submission

        document.getElementById("response").textContent = "";

        let form = document.getElementById("filterBy");

        // Create an object to hold the form values
        const formData = {
            user: form.user.value || null,
            group: form.group.value || null,
            state: form.state.value || null,
            recordState: form.recordState.value || null,
            recordId: form.recordId.value || null,
            transactionId: form.transactionId.value || null,
            startTime: form.startTime.value || null,
            endTime: form.endTime.value || null,
            order: form.order.value || null,
        };

        // Use the existing fetch structure
        fetch('/info', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(formData),
        })
            .then(resp => {
                if (!resp.ok) {
                    return resp.json().then(errorData => {
                        throw new Error(errorData.detail); // Throws error with message
                    });
                }
                return resp.json();
            })
            .then(data => {
                // Extract records and message from the response
                const records = data.records
                const message = data.message

                document.getElementById("message").textContent = message;

                // Clear existing table rows
                let tableBody = document.querySelector("#recordTable tbody");
                tableBody.innerHTML = "";

                // Recreate the header row
                const headerRow = `
                <tr>
                    <th>user</th>
                    <th>group</th>
                    <th>id</th>
                    <th>action</th>
                    <th>job label</th>
                    <th>state</th>
                    <th>last update</th>
                </tr>
            `;
                tableBody.insertAdjacentHTML('afterbegin', headerRow);

                // Populate the table with new records
                records.forEach(record => {
                    let row = document.createElement("tr");
                    row.classList.add("hover");
                    row.onclick = () => activateComplexView(`record_${record.id}`);

                    row.innerHTML = `
                    <td>${record.user || ''}</td>
                    <td>${record.group || ''}</td>
                    <td>${record.id || ''}</td>
                    <td>${record.api_action || ''}</td>
                    <td>${record.job_label || ''}</td>
                    <td>${record.state || ''}</td>
                    <td>${record.creation_time || ''}</td>
                    <input type="hidden" id="record_${record.id}" value='${JSON.stringify(record)}'>
                `;
                    tableBody.appendChild(row);
                });
            })
            .catch(error => {
                // Display the error message in the designated area
                document.getElementById("response").textContent = error.message;
            });
    });
});

function toggleVisibility() {
    // When called switches between complex and simple view

    const simpleElements = document.querySelectorAll(".simple");
    simpleElements.forEach((element) => {
        element.style.display = element.style.display === "none" ? "block" : "none";
    });

    const complexElements = document.querySelectorAll(".complex");
    complexElements.forEach((element) => {
        element.style.display = element.style.display === "block" ? "none" : "block";
    });
}

function buildTable(records, values, headers, id) {
    // Builds tables for the complex view

    // Clear any existing content from the table body
    let tableContainer = document.getElementById(id);
    tableContainer.innerHTML = "";

    // Create a new table element
    let newTable = document.createElement("table");

    // Create a header row
    let headerRow = document.createElement("tr");
    headers.forEach(header => {
        let th = document.createElement("th");
        th.textContent = header;
        headerRow.appendChild(th);
    });
    newTable.appendChild(headerRow);

    // Create data rows
    records.forEach(record => {
        let dataRow = document.createElement("tr");
        values.forEach(value => {
            let td = document.createElement("td");

            // Special handling for "failed_files"
            if (value === "failed_files") {
                if (record.failed_files && record.failed_files.length > 0) {
                    // Create a nested table for failed files
                    let failedTable = document.createElement("table");
                    let failedHeaderRow = document.createElement("tr");

                    // Add headers for the failed files table
                    ["File Name"].forEach(failedHeader => {
                        let th = document.createElement("th");
                        th.textContent = failedHeader;
                        failedHeaderRow.appendChild(th);
                    });
                    failedTable.appendChild(failedHeaderRow);

                    // Create rows for each failed file
                    record.failed_files.forEach(failedFile => {
                        let failedRow = document.createElement("tr");
                        let failedFileCell = document.createElement("td");
                        failedFileCell.textContent = failedFile;  // Add the failed file name
                        failedRow.appendChild(failedFileCell);
                        failedTable.appendChild(failedRow);
                    });

                    // Append the nested failed files table to the cell
                    td.appendChild(failedTable);
                } else {
                    // If no failed files, show "None"
                    td.textContent = "None";
                }
            } else {
                // Normal cell content
                td.textContent = record[value];
            }
            dataRow.appendChild(td);
        });
        newTable.appendChild(dataRow);
    });

    // Append the new table to the container
    tableContainer.appendChild(newTable);
}


function activateComplexView(recordId) {
    // when the table row is clicked it builds the complex tables and makes them visable

    const recordStr = document.getElementById(recordId).value;
    const record = JSON.parse(recordStr);
    toggleVisibility()


    let complexValues = ["id", "user", "group", "api_action", "job_label", "transaction_id", "creation_time", "state"];
    let complexHeaders = ["id", "user", "group", "action", "job label", "transaction id", "last update", "state"];
    buildTable([record], complexValues, complexHeaders, "complexTable")

    if (record.warnings && record.warnings.length > 0) {
        let warningValues = ["id", "warning"];
        buildTable(record, warningValues, warningValues, "warnings")
    } else {
        // TODO test with warnings (does this need to be cleared each time?)
        document.getElementById("warnings").textContent = "None";
    }

    let subValues = ["id", "sub_id", "state", "last_updated", "failed_files"];
    let subHeaders = ["id", "sub_id", "state", "last updated", "failed files"];
    buildTable(record["sub_records"], subValues, subHeaders, "subRecords")
}