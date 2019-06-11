'use strict'
var expect = require('chai').expect;
var assert = require('chai').assert;
var request = require('request');

function Get(url, para) {
    return new Promise ((resolve, reject) => {
        request.post({
            url: "http://127.0.0.1:4001" + url,
            method: "GET",
            json: true,
            headers: {
                "content-type": "application/json",
            },
            body: para
        },function(err,response,body){
            console.log('返回结果：');
            if(!err && response.statusCode == 200){
                if(body!=='null'){
                    console.log(body);
                    resolve(body);
                }
            }
        });
    });
}

describe('底层API测试',()=>{
    // describe('根据高度，获取某个区块信息',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/blockByHeight/BSV/4000");
    //         return result;
    //     });
    // });
    // describe('根据区块hash值，获取区块信息',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/blockByHash/BSV/0000000071966c2b1d065fd446b1e485b2c9d9594acd2007ccbd5441cfc89444");
    //         return result;
    //     });
    // });
    // describe('获取节点总体数据',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/blockchaininfo/BSV");
    //         return result;
    //     });
    // });
    // describe('获取最近的区块',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/bestblock/BSV/0/15");
    //         return result;
    //     });
    // });
    describe('根据区块高度获取预览交易数据',()=>{
        it('返回0,成功',async () =>{
            let result = await Get("/api/block/browser_txs/BSV/000000003a1f2355922617c1ce8c0b0ee6c08e2614ccc6d4a898191fe3f1c237/0/20");
            var b = new Buffer(result.Coinbase, 'base64');
            console.log(b);
            return result;
        });
    });
    // describe('根据交易hash获取交易数据',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/tx/BSV/07612439f1f75775377829d8394c7bb4264b3fe51712ddc65f81f28bdf4a392c");
    //         console.log(result["vin"]);
    //         return result;
    //     });
    // });
    // describe('根据交易hash获取浏览器交易数据',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/browser_tx/BSV/07612439f1f75775377829d8394c7bb4264b3fe51712ddc65f81f28bdf4a392c");
    //         return result;
    //     });
    // });
    // describe('查询地址余额',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/unspentinfo/BSV/1AiBYt8XbsdyPAELFpcSwRpu45eb2bArMf");
    //         return result;
    //     });
    // });
    // describe('根据地址获取浏览器需要tx交易预览数据',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/address/browser_txs/BSV/1AiBYt8XbsdyPAELFpcSwRpu45eb2bArMf/0/20");
    //         return result;
    //     });
    // });
    // describe('根据地址获取可以花费的交易',()=>{
    //     it('返回0,成功',async () =>{
    //         let result = await Get("/api/utxo/BSV/1C5epYhuhg2UDHgR3BQmEkcMRUBxwbyUe4");
    //         return result;
    //     });
    // });
})


