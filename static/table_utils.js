$(document).ready(function() {
    function initializeDataTable(tableId, maxHeight = '300px') {
        var dataTable = $(tableId).DataTable({
            searching: true, // Enable searching
            lengthChange: false,
            info: false,
            paging: false,
            "scrollY": maxHeight,
            "fixedHeader": true,
            order: [[0, 'desc']],
            initComplete: function () {
                this.api()
                    .columns()
                    .every(function () {
                        let column = this;
                        let title = $(column.header()).text(); // Get column header text

                        // Create input element
                        let input = document.createElement('input');
                        input.placeholder = "Search " + title;

                        // Replace footer content with input
                        $(column.footer()).html(input);

                        // Event listener for user input
                        $(input).on('keyup', function () {
                            if (column.search() !== this.value) {
                                column.search(this.value).draw();
                            }
                        });
                    });
            }
        });
    }

    initializeDataTable('#flights');
    initializeDataTable('#flight_aggregation' ); // Set a different maxHeight here if needed
});
