import debugCore from 'debug';

const debug = debugCore('verdaccio:plugin:aws-s3-storage:config');

// Reads the named environment variable if it is set, else keeps the literal value
export default function setConfigValue<T extends string | undefined>(configValue: T): T | string {
  if (configValue === undefined) return configValue;
  const envValue = process.env[configValue];
  if (envValue) {
    debug('resolved %o from env var', configValue);
    return envValue;
  }
  return configValue;
}
