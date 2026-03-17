/**
 * @section imports:internals
 */

import config from "../config.ts";

/**
 * @section types
 */

export type PackageInfo = { packageName: string };

type PackageInfoServiceOptions = { packageName: string };

/**
 * @section class
 */

export class PackageInfoService {
  /**
   * @section private:attributes
   */

  private readonly packageName: string;

  /**
   * @section constructor
   */

  public constructor(options: PackageInfoServiceOptions) {
    this.packageName = options.packageName;
  }

  /**
   * @section factory
   */

  public static createDefault(): PackageInfoService {
    return new PackageInfoService({ packageName: config.PACKAGE_NAME });
  }

  /**
   * @section public:methods
   */

  public readPackageInfo(): PackageInfo {
    return { packageName: this.packageName };
  }
}
