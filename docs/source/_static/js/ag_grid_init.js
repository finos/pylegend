/*
    # Copyright 2026 Goldman Sachs
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    #      http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
 */

document.addEventListener('DOMContentLoaded', function () {
    const tables = document.querySelectorAll('table.dataframe');
    if (tables.length === 0) return;

    function initTable(table, agGridModule) {
        // Parse headers
        const headers = [];
        const headRows = table.querySelectorAll('thead tr');
        const headerRow = headRows.length > 0 ? headRows[headRows.length - 1] : table;
        const validCells = headerRow.querySelectorAll('th');

        validCells.forEach(function (th, index) {
            let headerText = th.innerText.trim();
            if (!headerText) {
                headerText = 'Index ' + (index > 0 ? index : '');
            }

            headers.push({
                field: 'col_' + index,
                headerName: headerText,
                sortable: true,
                filter: true,
                resizable: true,
                flex: 1,
                minWidth: 100
            });
        });

        // Parse rows
        const rowData = [];
        const tbodyRows = table.querySelectorAll('tbody tr');
        tbodyRows.forEach(function (tr) {
            const row = {};
            const cells = tr.querySelectorAll('th, td');

            let hIndex = 0;
            cells.forEach(function (cell) {
                if (headers[hIndex]) {
                    const value = cell.innerText.trim();
                    const numValue = Number(value);
                    row[headers[hIndex].field] = isNaN(numValue) || value === '' ? value : numValue;
                }
                hIndex++;
            });
            rowData.push(row);
        });

        // Create container for ag-grid
        const gridDiv = document.createElement('div');

        const isDarkMode = document.body.classList.contains('dark-mode');
        gridDiv.className = isDarkMode ? 'ag-theme-alpine-dark' : 'ag-theme-alpine';

        gridDiv.style.width = '100%';
        gridDiv.style.margin = '20px 0';
        gridDiv.style.height = rowData.length > 15 ? '400px' : (rowData.length * 45 + 100) + 'px';

        table.style.display = 'none';

        let parent = table.parentNode;
        if (parent.tagName === 'DIV' && parent.classList.contains('table-wrapper')) {
            parent.style.display = 'none';
            parent.parentNode.insertBefore(gridDiv, parent);
        } else {
            table.parentNode.insertBefore(gridDiv, table);
        }

        const gridOptions = {
            columnDefs: headers,
            rowData: rowData,
            pagination: true,
            paginationPageSize: 10,
        };

        if (agGridModule) {
            try {
                if (agGridModule.createGrid) {
                    agGridModule.createGrid(gridDiv, gridOptions);
                } else {
                    new agGridModule.Grid(gridDiv, gridOptions);
                }
            } catch (e) {
                console.error("Error creating grid:", e);
                new agGridModule.Grid(gridDiv, gridOptions);
            }
        }

        if (!window.agGrids) window.agGrids = [];
        window.agGrids.push({ div: gridDiv, options: gridOptions });
    }

    // Function to load agGrid dynamically working seamlessly with and without requirejs
    function loadAgGridAndInitialize() {
        if (typeof window.agGrid !== 'undefined') {
            tables.forEach(function (table) { initTable(table, window.agGrid); });
            return;
        }

        if (typeof require !== 'undefined' && typeof requirejs !== 'undefined') {
            require.config({
                paths: {
                    'ag-grid-community': 'https://cdn.jsdelivr.net/npm/ag-grid-community@31.3.2/dist/ag-grid-community.min'
                }
            });
            require(['ag-grid-community'], function (agGridModule) {
                window.agGrid = agGridModule || window.agGrid;
                tables.forEach(function (table) { initTable(table, window.agGrid); });
            });
        } else {
            const script = document.createElement('script');
            script.src = 'https://cdn.jsdelivr.net/npm/ag-grid-community@31.3.2/dist/ag-grid-community.min.js';
            script.onload = function () {
                if (typeof window.agGrid !== 'undefined') {
                    tables.forEach(function (table) { initTable(table, window.agGrid); });
                } else {
                    console.error("AG Grid script loaded, but window.agGrid is still undefined.");
                }
            };
            document.head.appendChild(script);
        }
    }

    loadAgGridAndInitialize();

    // Handle dark mode toggle for ag-grids instantly using MutationObserver
    const observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            if (mutation.attributeName === 'class') {
                const isDarkMode = document.body.classList.contains('dark-mode');
                if (window.agGrids) {
                    window.agGrids.forEach(function (grid) {
                        grid.div.className = isDarkMode ? 'ag-theme-alpine-dark' : 'ag-theme-alpine';
                    });
                }
            }
        });
    });
    observer.observe(document.body, { attributes: true });
});
