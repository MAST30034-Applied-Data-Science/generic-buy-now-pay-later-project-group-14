const elts = document.getElementsByTagName("a");
console.log(elts);
console.log(elts.length);

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
