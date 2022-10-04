const {Template} = require("ejs");
const fs = require("fs");

const renderTop100 = async (req, res) => {
  try {
    const data = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/top100.json`)
    );
    res.render("main", {
      layout: "template",
      data,
    });
  } catch (err) {
    res.status(404).json({
      status: "fail",
      message: err,
    });
  }
};

const renderHealthService = async (req, res) => {
  try {
    const data = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/health_service.json`)
    );
    res.render("main", {
      layout: "template",
      data,
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
  renderHealthService,
};
