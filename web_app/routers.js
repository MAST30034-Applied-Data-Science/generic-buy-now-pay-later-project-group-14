const express = require("express");
const Router = express.Router();
const controllers = require("./Ctrler");

Router.get("/top100", controllers.renderTop100);
Router.get("/health-service", controllers.renderHealthService);

module.exports = Router;
