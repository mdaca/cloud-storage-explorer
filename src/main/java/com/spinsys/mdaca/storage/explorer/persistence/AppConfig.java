package com.spinsys.mdaca.storage.explorer.persistence;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="APP_CONFIG")
public class AppConfig {

	private String configKey;
	private String configValue;

	@Id
	@Column(name = "CONFIG_KEY")
	public String getConfigKey() {
		return configKey;
	}
	public void setConfigKey(String configKey) {
		this.configKey = configKey;
	}

	@Column(name = "CONFIG_VALUE", length = 2048)
	public String getConfigValue() {
		return configValue;
	}
	public void setConfigValue(String configValue) {
		this.configValue = configValue;
	}
}
