// ════════════════════════════════════════════════
// 水晶報告後台 - Google Apps Script
// 貼到 Google Sheets 的 Apps Script 後部署為網頁應用程式
// ════════════════════════════════════════════════
const SHEET_NAME = '客戶記錄';

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    writeCustomer(ss, data);
    return ContentService
      .createTextOutput(JSON.stringify({status:'ok'}))
      .setMimeType(ContentService.MimeType.JSON);
  } catch(err) {
    return ContentService
      .createTextOutput(JSON.stringify({status:'error',message:err.toString()}))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  return ContentService
    .createTextOutput(JSON.stringify({status:'ok'}))
    .setMimeType(ContentService.MimeType.JSON);
}

function writeCustomer(ss, data) {
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
    const headers = ['儲存時間','姓名','國曆生日','農曆生日','出生時間','手圍(cm)',
      '流年期間','適合顏色','不適合顏色','水晶搭配','數字搭配','國曆流年','農曆流年','流年建議'];
    sheet.appendRow(headers);
    const hr = sheet.getRange(1,1,1,headers.length);
    hr.setBackground('#9a7a4a');
    hr.setFontColor('#ffffff');
    hr.setFontWeight('bold');
    hr.setFontSize(11);
    sheet.setFrozenRows(1);
    [130,80,100,100,80,70,160,120,120,300,150,70,70,300]
      .forEach((w,i) => sheet.setColumnWidth(i+1, w));
  }
  const row = [
    data.savedAt||new Date().toLocaleString('zh-TW'),
    data.name,data.solar,data.lunar,data.btime,data.wrist,
    data.period,data.goodColors,data.badColors,
    (data.crystals||'').replace(/\n/g,'；'),
    (data.numbers||'').replace(/\n/g,'；'),
    data.lySolar,data.lyLunar,data.lyContent
  ];
  sheet.appendRow(row);
  const lastRow=sheet.getLastRow();
  if(lastRow%2===0) sheet.getRange(lastRow,1,1,row.length).setBackground('#faf6f0');
}
