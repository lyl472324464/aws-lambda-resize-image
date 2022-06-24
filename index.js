'use strict';

const http = require('http');
const https = require('https');
const querystring = require('querystring');

const AWS = require('aws-sdk');
const S3 = new AWS.S3({
  signatureVersion: 'v4',
});
const Sharp = require('sharp');


// set the S3 and API GW endpoints
// const BUCKET = "ivs-record"
// const BUCKET = 'ganbei-gtv-test'; // dev
// const BUCKET = 'gtv-mc-videos'; // fat
// const BUCKET = 'gtv-datalake-crawler'; // datalake
// const BUCKET = "gtv-mc-ivs-sit"
// const BUCKET = "gtv-mc-videos-sit"
// const BUCKET = 'aiai-mc-videos-stag'; // staging
// const BUCKET = 'aiai-datalake';
// const BUCKET = 'stag-ivs';
const BUCKET = 'aiai-mc-videos-prod';

const PROGRESS_BAR_PREFIX = 'progressBarThumbnails';
const PREVIEW_PREFIX = 'previewThumbnails';

const generateIvsProgressBarThumbnail = (callback, response, key, progressBarThumbnailMatch, secondsPerImg, imgInterval, imgPerThumbnail, imgPerWidth, imgHeight, liveIsEnded) => {
  const ORIGINTHUMBNAILFOLDER = 'thumbnails';
  const SECONDSPERIMG = secondsPerImg; // seconds per image
  const IMGINTERVAL = imgInterval; // 每多少秒取一张图片
  const IMGPERTHUMBNAIL = imgPerThumbnail;
  const IMGPERWIDTH = imgPerWidth;
  // const IMGPERHEIGHT = 5;
  const IMGHEIGHT = imgHeight;
  const THUMBNAILTYPE = 'webp';
  S3.listObjectsV2(
    {
      Bucket: BUCKET, /* required */
      Prefix: progressBarThumbnailMatch[1] + "/" + ORIGINTHUMBNAILFOLDER + "/",
      MaxKeys: 1
    }, 
    function(err, data) {
      if (err) {
        // console.log(err, err.stack); // an error occurred
        callback(null, response);
      } else {
        // console.log(data);           // successful response
        if (data.Contents.length == 0) {
          console.log("No thumbnail found for video: " + progressBarThumbnailMatch[1]);
          callback(null, response);
        } else {
          let thumbnailMatch = data.Contents[0].Key.match(/(.*)\/thumb(.*)\.(.*)/);
          let startNumber = parseInt(parseInt(progressBarThumbnailMatch[3]) * IMGPERTHUMBNAIL * IMGINTERVAL / SECONDSPERIMG);
          let thumbnailNumsString = [];
          for(let i = 0; i < IMGPERTHUMBNAIL; i++) {
            let thumbnailNum = parseInt(startNumber + i * IMGINTERVAL / SECONDSPERIMG);
            let thumbnailNumString = thumbnailNum.toString();
            thumbnailNumsString.push(thumbnailNumString);
          }
          // console.log('需要的照片：', thumbnailNumsString, thumbnailMatch, thumbnailMatch);
          let promises = [];
          for(let i = 0; i < thumbnailNumsString.length; i++){
            promises.push(
              S3.getObject({ Bucket: BUCKET, Key: thumbnailMatch[1] + "/thumb" + thumbnailNumsString[i] + "." + thumbnailMatch[3] }).promise().catch(err => {return 'error';})
            )
          }
          Promise.all(promises).then(originValues => {
            let values = []
            let if_generate = true
            for(let o in originValues){
              if(originValues[o] != 'error') {
                values.push(originValues[o])
              }else{
                if (!liveIsEnded){
                  if_generate = false
                }
                break;
              }
            }
            // console.log(values.length);
            if(values.length > 0 && if_generate) {
              let imgOriginBuffers = [];
              for(let v in values){
                imgOriginBuffers.push(values[v].Body);
              }
              // 取第一张图片获取长和宽
              Sharp(imgOriginBuffers[0]).metadata().then(function(metadata) {
                let imgHeight = IMGHEIGHT;
                let imgWidth = parseInt(metadata.width * imgHeight / metadata.height);
                let resizePromises = []
                for(let i = 0; i < imgOriginBuffers.length; i++) {
                  resizePromises.push(
                    Sharp(imgOriginBuffers[i]).resize(imgWidth, imgHeight).toBuffer()
                  )
                }
                Promise.all(resizePromises).then(resizeBuffers => {
                  let finalThumbnailWidth = IMGPERWIDTH * imgWidth;
                  let finalThumbnailHeight = Math.ceil(resizeBuffers.length / IMGPERWIDTH) * imgHeight;
                  // let needPushNum = IMGPERWIDTH - resizeBuffers.length % IMGPERWIDTH
                  // for(let i = 0; i < needPushNum; i++) {
                  //   resizeBuffers.push(resizeBuffers[resizeBuffers.length - 1]);
                  // }
                  let compositeImages = []
                  for(let i = 0; i < resizeBuffers.length; i++) {
                    compositeImages.push({
                      input: resizeBuffers[i],
                      left: i % IMGPERWIDTH * imgWidth,
                      top: parseInt(i / IMGPERWIDTH) * imgHeight
                    })
                  }
                  // console.log('照片位置：', compositeImages);
                  Sharp({
                    create: {
                      width: finalThumbnailWidth,
                      height: finalThumbnailHeight,
                      channels: 4,
                      background: { r: 255, g: 255, b: 255, alpha: 0.5 }
                    }
                  })
                  .composite(compositeImages)
                  .webp()
                  .toBuffer()
                  .then(buffer => {
                    // save the resized object to S3 bucket with appropriate object key.
                    S3.putObject({
                      Body: buffer,
                      Bucket: BUCKET,
                      ContentType: 'image/' + THUMBNAILTYPE,
                      CacheControl: 'max-age=31536000',
                      Key: key,
                      StorageClass: 'STANDARD'
                    }).promise()
                      // even if there is exception in saving the object we send back the generated
                      // image back to viewer below
                      .catch(() => { console.log("Exception while writing resized image to bucket") });
            
                    // generate a binary response with resized image
                    response.status = 200;
                    response.body = buffer.toString('base64');
                    response.bodyEncoding = 'base64';
                    response.headers['content-type'] = [{ key: 'Content-Type', value: 'image/' + THUMBNAILTYPE }];
                    callback(null, response);
                  })
                  .catch(err => {
                    console.log("Exception while reading source image :%s", err);
                    response.headers['content-type'] = [{ key: 'Content-Type', value: ("Exception while reading source image :%s", err) }];
                    callback(null, response);
                  });
                }).catch(reason => {
                  console.log(reason)
                  callback(null, response);
                }); 
              })
            } else {
              callback(null, response);
            }
          }).catch(reason => {
            console.log(reason)
            callback(null, response);
          });                    
        }
      }   
    }  
  )
}

const generateUserUploadProgressBarThumbnail = (callback, response, key, progressBarThumbnailMatch, secondsPerImg, imgInterval, imgPerThumbnail, imgPerWidth, imgHeight) => {
  const ORIGINTHUMBNAILFOLDER = 'thumbs'
  const SECONDSPERIMG = secondsPerImg; // seconds per image
  const IMGINTERVAL = imgInterval; // 每多少秒取一张图片
  const IMGPERTHUMBNAIL = imgPerThumbnail;
  const IMGPERWIDTH = imgPerWidth;
  // const IMGPERHEIGHT = 5;
  const IMGHEIGHT = imgHeight;
  const THUMBNAILTYPE = 'webp';
  S3.listObjectsV2(
    {
      Bucket: BUCKET, /* required */
      Prefix: progressBarThumbnailMatch[1] + "/" + ORIGINTHUMBNAILFOLDER + "/",
      MaxKeys: 1
    }, 
    function(err, data) {
      if (err) {
        // console.log(err, err.stack); // an error occurred
        callback(null, response);
      } else {
        // console.log(data);           // successful response
        if (data.Contents.length == 0) {
          console.log("No thumbnail found for video: " + progressBarThumbnailMatch[1]);
          callback(null, response);
        } else {
          let thumbnailMatch = data.Contents[0].Key.match(/(.*)\.(.*)\.(.*)/);
          let startNumber = parseInt(parseInt(progressBarThumbnailMatch[3]) * IMGPERTHUMBNAIL * IMGINTERVAL / SECONDSPERIMG);
          let thumbnailNumsString = [];
          for(let i = 0; i < IMGPERTHUMBNAIL; i++) {
              let thumbnailNum = parseInt(startNumber + i * IMGINTERVAL / SECONDSPERIMG);
              let thumbnailNumString = thumbnailNum.toString();
              let thumbnailNumStringLength = thumbnailNumString.length;
              for(let l = 0; l < 7 - thumbnailNumStringLength; l++) {
                thumbnailNumString = "0" + thumbnailNumString;
              }
              thumbnailNumsString.push(thumbnailNumString);
          }
          // console.log('需要的照片：', thumbnailNumsString);
          let promises = [];
          for(let i = 0; i < thumbnailNumsString.length; i++){
            promises.push(
              S3.getObject({ Bucket: BUCKET, Key: thumbnailMatch[1] + "." + thumbnailNumsString[i] + "." + thumbnailMatch[3] }).promise().catch(err => {return 'error';})
            )
          }
          Promise.all(promises).then(originValues => {
            let values = []
            for(let o in originValues){
              if(originValues[o] != 'error') {
                values.push(originValues[o])
              }else{
                break;
              }
            }
            if(values.length > 0) {
              let imgOriginBuffers = [];
              for(let v in values){
                imgOriginBuffers.push(values[v].Body);
              }
              // 取第一张图片获取长和宽
              Sharp(imgOriginBuffers[0]).metadata().then(function(metadata) {
                let imgHeight = IMGHEIGHT;
                let imgWidth = parseInt(metadata.width * imgHeight / metadata.height);
                let resizePromises = []
                for(let i = 0; i < imgOriginBuffers.length; i++) {
                  resizePromises.push(
                    Sharp(imgOriginBuffers[i]).resize(imgWidth, imgHeight).toBuffer()
                  )
                }
                Promise.all(resizePromises).then(resizeBuffers => {
                  let finalThumbnailWidth = IMGPERWIDTH * imgWidth;
                  let finalThumbnailHeight = Math.ceil(resizeBuffers.length / IMGPERWIDTH) * imgHeight;
                  // let needPushNum = IMGPERWIDTH - resizeBuffers.length % IMGPERWIDTH
                  // for(let i = 0; i < needPushNum; i++) {
                  //   resizeBuffers.push(resizeBuffers[resizeBuffers.length - 1]);
                  // }
                  let compositeImages = []
                  for(let i = 0; i < resizeBuffers.length; i++) {
                    compositeImages.push({
                      input: resizeBuffers[i],
                      left: i % IMGPERWIDTH * imgWidth,
                      top: parseInt(i / IMGPERWIDTH) * imgHeight
                    })
                  }
                  // console.log('照片位置：', compositeImages);
                  Sharp({
                    create: {
                      width: finalThumbnailWidth,
                      height: finalThumbnailHeight,
                      channels: 4,
                      background: { r: 255, g: 255, b: 255, alpha: 0.5 }
                    }
                  })
                  .composite(compositeImages)
                  .webp()
                  .toBuffer()
                  .then(buffer => {
                    // save the resized object to S3 bucket with appropriate object key.
                    S3.putObject({
                      Body: buffer,
                      Bucket: BUCKET,
                      ContentType: 'image/' + THUMBNAILTYPE,
                      CacheControl: 'max-age=31536000',
                      Key: key,
                      StorageClass: 'STANDARD'
                    }).promise()
                      // even if there is exception in saving the object we send back the generated
                      // image back to viewer below
                      .catch(() => { console.log("Exception while writing resized image to bucket") });
            
                    // generate a binary response with resized image
                    response.status = 200;
                    response.body = buffer.toString('base64');
                    response.bodyEncoding = 'base64';
                    response.headers['content-type'] = [{ key: 'Content-Type', value: 'image/' + THUMBNAILTYPE }];
                    callback(null, response);
                  })
                  .catch(err => {
                    console.log("Exception while reading source image :%s", err);
                    response.headers['content-type'] = [{ key: 'Content-Type', value: ("Exception while reading source image :%s", err) }];
                    callback(null, response);
                  });
                }).catch(reason => {
                  console.log(reason)
                  callback(null, response);
                }); 
              })
            } else {
              callback(null, response);
            }
          }).catch(reason => {
            console.log(reason)
            callback(null, response);
          });                    
        }
      }   
    }  
  );
}

const generateSpecifiedDimensionImg = (callback, response, key) => {
  // Ex: key=image/20211230/3/21/36/22766b4eaf3b4b06a53e5d9df80e3607_png_200_200.webp
  let originalKey, match, width, height, quality, lossless, requiredFormat, imageName, originFormat;

  match = key.match(/(.*)_(.*)_(.*)_(.*)_(.*)_(.*)\.(.*)/);
  imageName = match[1];
  // correction for jpg required for 'Sharp'
  originFormat = match[2];
  width = parseInt(match[3], 10);
  height = parseInt(match[4], 10);
  quality = parseInt(match[5], 10);
  lossless = parseInt(match[6], 10);
  requiredFormat = match[7] == "jpg" ? "jpeg" : match[7];

  originalKey = imageName + "." + originFormat;
  console.log(originalKey)
  let dimension;

  if (width == 0) {
    dimension = { height: height, withoutEnlargement: true };
  } else {
    if (height == 0) {
      dimension = { width: width, withoutEnlargement: true };
    } else {
      dimension = { width: width, height: height, withoutEnlargement: true };
    }
  }

  // console.log(key, originalKey)
  let sharp_options;
  if (requiredFormat == "webp") {
    sharp_options = {
      quality: quality,
      lossless: lossless == 1 ? true : false,
      nearlossless: lossless == 0 ? true : false,
      smartSubsample: false
    }
    // console.log(sharp_options)
  } else {
    sharp_options = {
      quality: quality
    }
  }
  // get the source image file
  S3.getObject({ Bucket: BUCKET, Key: originalKey }).promise()
    // perform the resize operation
    .then(data => Sharp(data.Body)
      .resize(dimension)
      .toFormat(requiredFormat, sharp_options)
      .toBuffer()
    )
    .then(buffer => {
      // save the resized object to S3 bucket with appropriate object key.
      S3.putObject({
        Body: buffer,
        Bucket: BUCKET,
        ContentType: 'image/' + requiredFormat,
        CacheControl: 'max-age=31536000',
        Key: key,
        StorageClass: 'STANDARD'
      }).promise()
        // even if there is exception in saving the object we send back the generated
        // image back to viewer below
        .catch(() => { console.log("Exception while writing resized image to bucket") });

      // generate a binary response with resized image
      response.status = 200;
      response.body = buffer.toString('base64');
      response.bodyEncoding = 'base64';
      response.headers['content-type'] = [{ key: 'Content-Type', value: 'image/' + requiredFormat }];
      callback(null, response);
    })
    .catch(err => {
      console.log("Exception while reading source image :%s", err);
      response.headers['content-type'] = [{ key: 'Content-Type', value: ("Exception while reading source image :%s", err) }];
      callback(null, response);
    });
}

exports.handler = (event, context, callback) => {
  let response = event.Records[0].cf.response;

  // console.log("Response status code :%s", response.status);

  //check if image is not present
  if (response.status == 403) {
    let request = event.Records[0].cf.request;
    let params = querystring.parse(request.querystring);    
    // read the required path
    let path = request.uri;
    let key = path.substring(1);
    let progressBarThumbnailMatch = key.match(/(.*)\/(.*)\/(.*)\.(.*)/)
    if (progressBarThumbnailMatch && progressBarThumbnailMatch[2] && progressBarThumbnailMatch[2] == PROGRESS_BAR_PREFIX) {
      if (key.substring(0, 4) == "ivs/") {
        // 判断直播是否结束
        const live_end_json = key.match(/(.*)\/(.*)\/(.*)\/(.*)\.(.*)/)[1] + "/events/recording-ended.json"
        S3.getObject({Bucket: BUCKET, Key: live_end_json}).promise().then(() => {
          generateIvsProgressBarThumbnail(callback, response, key, progressBarThumbnailMatch, 10, 10, 25, 5, 90, true); //ivs thumbnails
        }).catch(() => {
          generateIvsProgressBarThumbnail(callback, response, key, progressBarThumbnailMatch, 10, 10, 25, 5, 90, false);
        })
      } else {
        generateUserUploadProgressBarThumbnail(callback, response, key, progressBarThumbnailMatch, 10, 10, 25, 5, 90); //UserUpload thumbnails
      }      
    } else {
      if (progressBarThumbnailMatch && progressBarThumbnailMatch[2] && progressBarThumbnailMatch[2] == PREVIEW_PREFIX) {
        if (key.substring(0, 4) == "ivs/") {
          const live_end_json = key.match(/(.*)\/(.*)\/(.*)\/(.*)\.(.*)/)[1] + "/events/recording-ended.json"
          S3.getObject({Bucket: BUCKET, Key: live_end_json}).promise().then(() => {
            generateIvsProgressBarThumbnail(callback, response, key, progressBarThumbnailMatch, 10, 10, 25, 5, 253, true); //ivs thumbnails
          }).catch(() => {
            generateIvsProgressBarThumbnail(callback, response, key, progressBarThumbnailMatch, 10, 10, 25, 5, 253, false);
          })
        } else {
          generateUserUploadProgressBarThumbnail(callback, response, key, progressBarThumbnailMatch, 10, 10, 25, 5, 253); //UserUpload thumbnails
        }      
      } else {
        if (params.d || params.w || params.h){
          generateSpecifiedDimensionImg(callback, response, key)
        } else {
          callback(null, response);
        }
      }
    }    
  } // end of if block checking response statusCode
  else {
    // allow the response to pass through
    callback(null, response);
  }
};