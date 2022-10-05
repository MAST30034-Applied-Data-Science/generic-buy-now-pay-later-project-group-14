jQuery(function ($) {
  switch (window.location.href) {
    case "http://localhost:3000/v1/top100":
      $(".top100").addClass("selected");
      break;
    case "http://localhost:3000/v1/health-service":
      $(".health-service").addClass("selected");
      break;
    case "http://localhost:3000/v1/recreational-goods":
      $(".recreational-goods").addClass("selected");
      break;
    case "http://localhost:3000/v1/personal-household":
      $(".personal-household").addClass("selected");
      break;
    case "http://localhost:3000/v1/technical-machinery":
      $(".technical-machinery").addClass("selected");
      break;
  }
});

const merchant = document.getElementsByClassName("card");
for (let i = 0; i < merchant.length; i++) {
  merchant[i].onmousedown = function () {
    merchant[i].classList.toggle("select-merchant");
  };
}

const overlay = document.querySelector(".overlay");

overlay.addEventListener("click", () => {
  for (let i = 0; i < merchant.length; i++) {
    merchant[i].classList.remove("select-merchant");
  }
});

////////////////////////////
//FORMAT OF NUMBER PRESENTED
////////////////////////////
const round2 = function (originalNum) {
  return (Math.round(originalNum * 100) / 100).toFixed(2);
};

const rankNums = document.getElementsByClassName("rankNum");
const consumerNums = document.getElementsByClassName("consumerNum");
const numTransaction = document.getElementsByClassName("numTransaction");
const totalRevenue = document.getElementsByClassName("totalRevenue");
const scoreNum = document.getElementsByClassName("scoreNum");

for (let i = 0; i < 100; i++) {
  rankNums[i].textContent = Math.trunc(rankNums[i].textContent);
  consumerNums[i].textContent = Math.trunc(consumerNums[i].textContent);
  numTransaction[i].textContent = round2(numTransaction[i].textContent);
  totalRevenue[i].textContent = round2(totalRevenue[i].textContent);
  scoreNum[i].textContent = round2(scoreNum[i].textContent);
}
