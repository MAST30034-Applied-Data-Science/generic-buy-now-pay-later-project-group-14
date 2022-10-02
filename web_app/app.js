const express = require("express");
const app = express();
const routers = require("./routers");
const port = 3000;
//Loads the handlebars module
const handlebars = require("express-handlebars");
//Sets our app to use the handlebars engine
//instead of app.set('view engine', 'handlebars');
app.set("view engine", "hbs");
//instead of app.engine('handlebars', handlebars({
app.engine(
  "hbs",
  handlebars.engine({
    layoutsDir: __dirname + "/views/layout/",
    //new configuration parameter
    extname: "hbs",
  })
);
app.use(express.static("public"));
app.get("/", (req, res) => {
  res.render("main", {layout: "template"});
});

app.use("/v1", routers);

app.listen(port, () => console.log(`App listening to port ${port}`));
