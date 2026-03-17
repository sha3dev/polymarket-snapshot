import Logger from "@sha3/logger";
import config from "./config.ts";

const LOGGER_NAME = config.PACKAGE_NAME.startsWith("@") ? config.PACKAGE_NAME.split("/")[1] || config.PACKAGE_NAME : config.PACKAGE_NAME;
const logger = new Logger({ loggerName: LOGGER_NAME });

export default logger;
