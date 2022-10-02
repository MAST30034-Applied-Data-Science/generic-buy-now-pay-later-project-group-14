const express = require("express");
const Router = express.Router();
const ctrlertop100 = require("./top100Ctrler");

Router.get("/top100", ctrlertop100.renderTop100);

module.exports = Router;
