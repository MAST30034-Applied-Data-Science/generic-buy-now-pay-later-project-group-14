jQuery(function ($) {
  switch (window.location.href) {
    case "http://localhost:3000/v1/top100":
      $(".top100").addClass("selected");
      break;
    case "http://localhost:3000/v1/health-service":
      $(".health-service").addClass("selected");
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
