package com.spinsys.mdaca.storage.explorer.provider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StorageProviderFactoryTest {

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testGetProviderS3() {
		StorageProvider provider = null;
		
		try {
			provider = StorageProviderFactory.getProvider(DriveType.S3);
			assertTrue(provider instanceof AWSS3StorageProvider);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	void testGetProviderSMB() {
		StorageProvider provider = null;
		
		try {
			provider = StorageProviderFactory.getProvider(DriveType.SMB);
			assertTrue(provider instanceof SMBStorageProvider);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	void testGetProviderS32() {
		StorageProvider provider = null;
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse mockedResponse = Mockito.mock(HttpServletResponse.class);
		when(mockedRequest.getAttribute(anyString())).thenReturn(null);
		
		try {
			provider = StorageProviderFactory.getProvider(DriveType.S3, mockedRequest);
			assertTrue(provider instanceof AWSS3StorageProvider);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	void testGetProviderSMB2() {
		StorageProvider provider = null;
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse mockedResponse = Mockito.mock(HttpServletResponse.class);
		when(mockedRequest.getAttribute(anyString())).thenReturn(null);
		
		try {
			provider = StorageProviderFactory.getProvider(DriveType.SMB, mockedRequest);
			assertTrue(provider instanceof SMBStorageProvider);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	@Test
	void testGetProviderGCS() {
		StorageProvider provider = null;
		
		try {
			provider = StorageProviderFactory.getProvider(DriveType.GCS);
			assertTrue(provider instanceof GoogleCloudStorageProvider);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	void testGetProviderGCS2() {
		StorageProvider provider = null;
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse mockedResponse = Mockito.mock(HttpServletResponse.class);
		when(mockedRequest.getAttribute(anyString())).thenReturn(null);
		
		try {
			provider = StorageProviderFactory.getProvider(DriveType.GCS, mockedRequest);
			assertTrue(provider instanceof GoogleCloudStorageProvider);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}



}
