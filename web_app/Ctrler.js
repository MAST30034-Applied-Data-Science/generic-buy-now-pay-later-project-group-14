const fs = require("fs");

const renderTop100 = async (req, res) => {
  try {
    const data = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/top100.json`)
    );
    res.render("top100", {
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
      fs.readFileSync(`${__dirname}/models/health_top10.json`)
    );
    res.render("otherTags", {
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

const renderRecreationalGoods = async (req, res) => {
  try {
    const data = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/recreational_top10.json`)
    );
    res.render("otherTags", {
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

const renderPersonalHouseHold = async (req, res) => {
  try {
    const data = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/personal_top10.json`)
    );
    res.render("otherTags", {
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

const renderTechnicalMachinery = async (req, res) => {
  try {
    const data = await JSON.parse(
      fs.readFileSync(`${__dirname}/models/technical_top10.json`)
    );
    res.render("otherTags", {
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
  renderRecreationalGoods,
  renderPersonalHouseHold,
  renderTechnicalMachinery,
};
