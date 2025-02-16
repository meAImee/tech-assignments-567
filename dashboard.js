document.addEventListener("DOMContentLoaded", function() {
    console.log(1)
    function fetchData(sensorType, chartId) {
        fetch(`/api/${sensorType}`)
            .then(response => response.json())
            .then(data => {
                const labels = data.map(item => item.timestamp);
                const values = data.map(item => item.value);
                new Chart(document.getElementById(chartId), {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: sensorType,
                            data: values,
                            borderColor: 'rgba(75, 192, 192, 1)',
                            borderWidth: 2,
                            fill: false
                        }]
                    }
                });
            });
    }
    fetchData("temperature", "temperatureChart");
    fetchData("humidity", "humidityChart");
    fetchData("light", "lightChart");
});
