const elts = document.getElementsByTagName("a");

for (let i = 0; i < elts.length; i++) {
  elts[i].onclick = function () {
    for (let j = 0; j < elts.length; j++) {
      if (elts[j].classList.contains("selected")) {
        elts[j].classList.remove("selected");
      }
    }
    elts[i].classList.add("selected");
  };
}

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
