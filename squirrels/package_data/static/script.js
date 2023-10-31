const productSelect = document.getElementById('product-select');
const datasetSelect = document.getElementById('dataset-select');
const generatedParamsDiv = document.getElementById('generated-parameters');

const tableContainers = document.getElementById('table-container');
const resultTable = document.getElementById("result-table");
const tableHeader = document.getElementById('table-header');
const tableBody = document.getElementById('table-body');

const loadingIndicator = document.getElementById('loading-indicator');

const loginModal = document.getElementById('login-modal');
const loginForm = document.getElementById('login-form');
const incorrectMessage = document.getElementById('incorrect');
let loginProcessor = null;

const usernameTxt = document.getElementById('username-txt');

const datasetsMap = new Map();
const parametersMap = new Map();

function loginEvent(event) {
    event.preventDefault();
    const formData = new FormData(loginForm);
    fetch(token_path, {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (response.status === 401) {
            incorrectMessage.style.display = 'block'
        } else {
            response.json()
            .then(data => {
                sessionStorage.setItem("jwt_token", data.access_token);
                sessionStorage.setItem("username", `"${data.username}"`);
                callJsonAPI(catalog_path, renderDatasetsSelection);
                updateUsername();
                closeLoginModal();
            })
        }
    })
}

async function updateUsername() {
    const username = sessionStorage.getItem("username");
    usernameTxt.innerHTML = (username === null) ? "guest" : username;
}

function openLoginModal() {
    loginModal.style.display = 'flex';
    loadingIndicator.style.display = 'none';
}

function closeLoginModal() {
    loginModal.style.display = 'none';
}

async function callJsonAPI(path, func) {
    loadingIndicator.style.display = 'flex';
    const jwt_token = sessionStorage.getItem("jwt_token");
    const response = await fetch(path, {
        headers: {
            'Authorization': `Bearer ${jwt_token}`
        }
    });

    if (response.status === 401) {
        alert("Unauthorized action. Please try to 'Authorize' first");
    } else if (response.status === 200) {
        const data = await response.json();
        func(data);
    } else {
        const error = `Unexpected response status: ${response.status}`;
        alert(error);
        console.log(error);
    }
    loadingIndicator.style.display = 'none';
}

function changeDatasetSelection() {
    tableContainers.style.display = 'none';
    parametersMap.clear()
    refreshParameters();
}

function renderDatasetsSelection(data) {
    productSelect.innerHTML = "";
    datasetSelect.innerHTML = "";

    const product = data.products[0];
    const option = document.createElement('option');
    option.value = product.name;
    option.textContent = product.label;
    productSelect.appendChild(option);

    const datasets = product.versions[0].datasets;
    datasets.forEach(resource => {
        const option = document.createElement('option');
        option.value = resource.name;
        option.textContent = resource.label;
        datasetSelect.appendChild(option);
        datasetsMap.set(option.value, resource);
    });
    console.log(datasets);
    if (datasets.length === 0) {
        alert("No datasets available! Authentication may be required to access.");
    } else {
        changeDatasetSelection();
    }
}

function refreshParameters(provoker = null) {
    const selectedDatasetValue = datasetSelect.value;
    const parametersPath = datasetsMap.get(selectedDatasetValue).parameters_path;
    const queryParameters = getQueryParams(provoker)
    const parametersRequest = parametersPath + '?' + queryParameters
    console.log('Parameters request:', parametersRequest)

    callJsonAPI(parametersRequest, (jsonResponse) => {
        jsonResponse.parameters.forEach(function(param) {
            parametersMap.set(param.name, param);
        })
        generatedParamsDiv.innerHTML = "";
        for (const param of parametersMap.values()) {
            const newDiv = document.createElement('div')

            const addLabel = function() {
                const paramLabel = document.createElement('label')
                paramLabel.innerHTML = param.label
                newDiv.appendChild(paramLabel)
            }

            if (param.widget_type === "DateParameter") {
                addLabel()
                const dateInput = document.createElement('input')
                dateInput.type = 'date'
                dateInput.id = param.name
                dateInput.value = param.selected_date
                dateInput.onchange = updateParameter
                newDiv.appendChild(dateInput)
            } else if (param.widget_type === "NumberParameter") {
                addLabel()
                const sliderInput = document.createElement('input')
                sliderInput.type = 'range'
                sliderInput.id = param.name
                sliderInput.min = param.min_value
                sliderInput.max = param.max_value
                sliderInput.step = param.increment
                sliderInput.value = param.selected_value
                
                const sliderValue = document.createElement('div')
                sliderValue.id = param.name + '_value'
                sliderValue.className = 'slider-value'
                sliderValue.innerText = param.selected_value

                sliderInput.oninput = function() {
                    sliderValue.innerText = this.value;
                }
                sliderInput.onchange = updateParameter

                newDiv.appendChild(sliderInput)
                newDiv.appendChild(sliderValue)
            } else if (param.widget_type === "NumRangeParameter") {
                // TODO
            } else if (param.widget_type === "SingleSelectParameter" && param.options.length > 0) {
                addLabel()
                const singleSelect = document.createElement('select');
                singleSelect.id = param.name;
                param.options.forEach(function(option) {
                    const selectOption = document.createElement('option');
                    selectOption.value = option.id;
                    if (option.id === param.selected_id) {
                        selectOption.selected = true;
                    }
                    selectOption.innerText = option.label;
                    singleSelect.appendChild(selectOption);
                });
                singleSelect.onchange = updateParameter
                newDiv.appendChild(singleSelect);
            } else if (param.widget_type === "MultiSelectParameter" && param.options.length > 0) {
                addLabel()
                const multiSelect = document.createElement('select');
                multiSelect.id = param.name;
                multiSelect.multiple = true;
                param.options.forEach(function(option) {
                    const selectOption = document.createElement('option');
                    selectOption.value = option.id;
                    if (param.selected_ids.includes(option.id)) {
                        selectOption.selected = true;
                    }
                    selectOption.innerText = option.label;
                    multiSelect.appendChild(selectOption);
                });
                multiSelect.onchange = updateParameter
                newDiv.appendChild(multiSelect);
            }
            generatedParamsDiv.appendChild(newDiv);
        }
    });
}

function updateParameter() {
    const param = parametersMap.get(this.id)
    if (param.widget_type === "DateParameter") {
        param.selected_date = this.value
    } else if (param.widget_type === "NumberParameter") {
        param.selected_value = this.value
    } else if (param.widget_type === "NumRangeParameter") {
        // TODO
    } else if (param.widget_type === "SingleSelectParameter") {
        param.selected_id = this.options[this.selectedIndex].value
    } else if (param.widget_type === "MultiSelectParameter") {
        param.selected_ids = [...this.selectedOptions].map(option => option.value)
    }
    
    if (param.trigger_refresh) {
        refreshParameters(param)
    }
}

function getQueryParams(provoker = null) {
    const queryParams = {}
    function addToQueryParams(key, value) {
        if (value.widget_type === "DateParameter") {
            queryParams[key] = value.selected_date
        } else if (value.widget_type === "NumberParameter") {
            queryParams[key] = value.selected_value
        } else if (value.widget_type === "NumRangeParameter") {
            // TODO
        } else if (value.widget_type === "SingleSelectParameter") {
            queryParams[key] = value.selected_id
        } else if (value.widget_type === "MultiSelectParameter") {
            result = JSON.stringify(value.selected_ids)
            if (result !== '') queryParams[key] = result
        }
    }
    if (provoker !== null) {
        addToQueryParams(provoker.name, provoker)
    }
    else {
        for (const [key, value] of parametersMap.entries()) {
            addToQueryParams(key, value)
        }
    }
    console.log(queryParams)
    return new URLSearchParams(queryParams)
}

function getDatasetResults() {
    const selectedDatasetValue = datasetSelect.value;
    const resultPath = datasetsMap.get(selectedDatasetValue).result_path;
    const resultRequest = resultPath + '?' + getQueryParams()
    console.log('Result request:', resultRequest)

    callJsonAPI(resultRequest, (jsonResponse) => {
        tableHeader.innerHTML = ''
        tableBody.innerHTML = ''

        // Create the table header row
        const headerRow = document.createElement('tr');
        jsonResponse.schema.fields.forEach(field => {
            const th = document.createElement('th');
            th.textContent = field.name;
            headerRow.appendChild(th);
        });
        tableHeader.appendChild(headerRow);

        // Create the table data rows
        jsonResponse.data.forEach(dataObject => {
            const row = document.createElement('tr');
            jsonResponse.schema.fields.forEach(field => {
                const td = document.createElement('td');
                td.textContent = dataObject[field.name];
                row.appendChild(td);
            });
            tableBody.appendChild(row);
        });

        tableContainers.style.display = 'block'
    });
}

function copyTable() {
    let text = "";

    for (let i = 0; i < resultTable.rows.length; i++) {
      for (let j = 0; j < resultTable.rows[i].cells.length; j++) {
        text += resultTable.rows[i].cells[j].innerHTML + "\t";
      }
      text += "\n";
    }

    navigator.clipboard.writeText(text).then(function() {
      alert("Table copied to clipboard!");
    }, function() {
      alert("Copying failed.");
    });
}


loginForm.addEventListener('submit', loginEvent);
loginForm.addEventListener('reset', closeLoginModal);
updateUsername();
