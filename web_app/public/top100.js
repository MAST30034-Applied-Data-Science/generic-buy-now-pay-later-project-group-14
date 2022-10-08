const merchant = document.getElementsByClassName("card");
for (let i = 0; i < merchant.length; i++) {
  merchant[i].onmousedown = function () {
    merchant[i].classList.toggle("select-merchant");
  };
}

document.addEventListener(
  "keydown",
  (event) => {
    for (let i = 0; i < merchant.length; i++) {
      merchant[i].classList.remove("select-merchant");
    }
  },
  false
);
