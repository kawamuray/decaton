export {
    drawGraph,
    destroyChart,
    showArg
}

import Chart from 'chart.js';

function showArg(a) {
    console.log("Arg.type2 = " + a.type);
}

function drawGraph(id, labels, data) {
    var ctx = document.getElementById(id);
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Throughput',
                data: data,
            }]
        },
        options: {
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    });    
}

function destroyChart(chart) {
    chart.destroy();
}
