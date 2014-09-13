package com.lealife.hsfs.info;

public class FileId2Data {
	private String fileId;
	private byte[] data;
    public FileId2Data() {
		// TODO Auto-generated constructor stub
	}
    public FileId2Data(String fileId, byte[] data) {
		// TODO Auto-generated constructor stub
        this.setFileId(fileId);
        this.setData(data);
	}
	public String getFileId() {
		return fileId;
	}
	public void setFileId(String fileId) {
		this.fileId = fileId;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
}
