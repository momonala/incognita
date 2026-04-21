$(document).ready(function () {
    function initializeDataTable(tableId, maxHeight = "300px") {
        $(tableId).DataTable({
            searching: true,
            lengthChange: false,
            info: false,
            paging: false,
            scrollY: maxHeight,
            fixedHeader: true,
            order: [[0, 'desc']],
            initComplete: function () {
                this.api().columns().every(function () {
                    const column = this;
                    const title = $(column.header()).text();
                    const input = document.createElement("input");
                    input.placeholder = "Search " + title;
                    $(column.footer()).empty().append(input);
                    input.addEventListener('input', function () {
                        if (column.search() !== input.value) {
                            column.search(input.value).draw();
                        }
                    });
                });
            }
        });
    }

    initializeDataTable("#flight_aggregation");
});
