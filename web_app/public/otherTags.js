const chart = document.querySelector(".chart");
const overlay = document.querySelector(".overlay");

const BNPL_revenue = document.querySelectorAll(".BNPL-revenue");
const num_transaction = document.querySelectorAll(".num-transaction");
const num_consumer = document.querySelectorAll(".num-consumer");
const label = [
  "2021-3",
  "2021-4",
  "2021-5",
  "2021-6",
  "2021-7",
  "2021-8",
  "2021-9",
  "2021-10",
  "2021-11",
  "2021-12",
  "2022-1",
  "2022-2",
  "2022-3",
  "2022-4",
  "2022-5",
  "2022-6",
  "2022-7",
  "2022-8",
  "2022-9",
  "2022-10",
];
const colors = ["#3e95cd", "#3e20cd", "#9f35cd"];
const datas = [BNPL_revenue, num_consumer, num_transaction];

const merchant = document.getElementsByClassName("card");
for (let i = 0; i < merchant.length; i++) {
  merchant[i].onmousedown = function (event) {
    chart.classList.remove("hide");
    overlay.classList.add("overlay-shade");
    new Chart(document.getElementById("line-chart"), {
      type: "line",
      data: {
        labels: label,
        datasets: [
          {
            label: "BNPL revenue",
            borderColor: colors[0],
            data: datas[0][i].textContent.split(",").map((str) => {
              return Number(str);
            }),
            fill: false,
          },
          {
            label: "Number of consumers",
            borderColor: colors[1],
            data: datas[1][i].textContent.split(",").map((str) => {
              return Number(str);
            }),
            fill: false,
          },
          {
            label: "Number of transactions",
            borderColor: colors[2],
            data: datas[2][i].textContent.split(",").map((str) => {
              return Number(str);
            }),
            fill: false,
          },
        ],
      },
      options: {
        legend: {
          display: true,
          text: ["1", "2", "3"],
          labels: {fontSize: 18},
        },
        scales: {
          yAxes: [
            {
              ticks: {
                fontSize: 14,
              },
            },
          ],
          xAxes: [
            {
              ticks: {
                fontSize: 14,
              },
            },
          ],
        },
        title: {
          display: true,
          text: "Merchant performance throughout the year",
          fontSize: 22,
        },
      },
    });
  };
}

const close = document.querySelector(".btn");

close.addEventListener("click", () => {
  chart.classList.add("hide");
  overlay.classList.remove("overlay-shade");
});
