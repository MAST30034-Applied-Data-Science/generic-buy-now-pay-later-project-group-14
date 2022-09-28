const {Template} = require("ejs");
const fs = require("fs");

const renderTop100 = async (req, res) => {
  try {
    const top100 = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/merchant.json`)
    );
    res.render("main", {
      layout: "template",
      top100,
    });
  } catch (err) {
    res.status(404).json({
      status: "fail",
      message: err,
    });
  }
};

module.exports = {
  renderTop100,
};
