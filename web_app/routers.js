const express = require("express");
const Router = express.Router();
const controllers = require("./Ctrler");

Router.get("/top100", controllers.renderTop100);
Router.get("/health-service", controllers.renderHealthService);
Router.get("/recreational-goods", controllers.renderRecreationalGoods);
Router.get("/personal-household", controllers.renderPersonalHouseHold);
Router.get("/technical-machinery", controllers.renderTechnicalMachinery);

module.exports = Router;
