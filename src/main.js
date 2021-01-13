const Apify = require('apify');
const {log, requestAsBrowser} = Apify.utils;
const {getName:countryName} = require('i18n-iso-countries');

const LATEST = "LATEST";

Apify.main(async () => {
    const { notificationEmail } = await Apify.getInput();
    const url = "https://covid19.who.int/";
    const kvStore = await Apify.openKeyValueStore("COVID-19-WHO-SPRINKLR");
    const dataset = await Apify.openDataset("COVID-19-WHO-SPRINKLR-HISTORY");
    const requestList = await Apify.openRequestList('LIST', [
        {
            url: 'https://covid19.who.int/page-data/index/page-data.json'
        }
    ]);

    if (notificationEmail) {
        await Apify.addWebhook({
            eventTypes: ['ACTOR.RUN.FAILED', 'ACTOR.RUN.TIMED_OUT'],
            requestUrl: `https://api.apify.com/v2/acts/mnmkng~email-notification-webhook/runs?token=${Apify.getEnv().token}`,
            payloadTemplate: `{"notificationEmail": "${notificationEmail}", "eventType": {{eventType}}, "eventData": {{eventData}}, "resource": {{resource}} }`,
        });
    }

    let dataByStates = {};
    let dataByRegions = {};
    let statesInfo = {};
    let lastUpdated = 'N/A';

    const crawler = new Apify.BasicCrawler({
        requestList,
        handleRequestFunction: async ({request}) => {
            let response;
            let body;
            let countryGroups;
            let regionGroups;
            response = await requestAsBrowser({
                url: request.url,
                json: true,
            });
            body = response.body;
            countryGroups = body.result.pageContext.rawDataSets.countryGroups;
            regionGroups = body.result.pageContext.rawDataSets.regionGroups;
            let countries = [];
            let countriesObj = [];
            countryGroups.forEach(countryObj => {
                let country = countryName(countryObj.value, 'en');
                let coutryCode = countryObj.value;
                if (!countries.includes(coutryCode) && typeof(country) != 'undefined') {
                    countries.push(coutryCode);
                    countriesObj.push(coutryCode);
                    countriesObj[coutryCode] = countryObj;
                }
            });
            countries.sort(function (a, b) {
                a = countryName(a, 'en');
                b = countryName(b, 'en');

                if (a > b) {
                    return 1;
                }
                if (b > a) {
                    return -1;
                }
                return 0;
            });
            countries.forEach(countryCode => {
                let country = countryName(countryCode, 'en');
                let countryRows = countriesObj[countryCode].data.rows;
                dataByStates[country] = [];
                statesInfo[country] = [];

                let countryInfo = {};
                body.result.pageContext.rawDataSets.transmissionData.rows.forEach(row => {
                    if (row.ISO_2_CODE === countryCode) {
                        countryInfo['whoRegion'] = row.WHO_REGION;
                        countryInfo['transmissionClassification'] = row.CLASSIFICATION;
                        countryInfo['date'] = new Date(row.DATE).toISOString();
                        statesInfo[country].push(countryInfo);
                    }
                });

                countryRows.forEach(row => {
                    let data = {};
                    data['date'] = new Date(row[0]).toISOString();
                    data['deceased'] = parseInt(row[3]);
                    data['deceasedNew'] = parseInt(row[2]);
                    data['confirmed'] = parseInt(row[8]);
                    data['confirmedNew'] = parseInt(row[7]);
                    data['deceasedLast7Days'] = parseInt(row[4]);
                    data['deceasedLast7DaysChange'] = parseFloat(row[5]);
                    data['deceasedPerMillion'] = parseFloat(row[6]);
                    data['casesLast7Days'] = parseInt(row[9]);
                    data['casesLast7DaysChange'] = parseFloat(row[10]);
                    data['casesPerMillion'] = parseFloat(row[11]);
                    dataByStates[country].push(data);
                });
            });
            let regions = [];
            let regionsObj = [];
            regionGroups.forEach(regionObj => {
                let regionCode = regionObj.value;
                if (!regions.includes(regionCode)) {
                    regions.push(regionCode);
                    regionsObj.push(regionCode);
                    regionsObj[regionCode] = regionObj;
                }
            });
            regions.sort();
            regions.forEach(regionCode => {
                let regionRows = regionsObj[regionCode].data.rows;
                dataByRegions[regionCode] = [];

                regionRows.forEach(row => {
                    let data = {};
                    data['date'] = new Date(row[0]).toISOString();
                    data['deceased'] = parseInt(row[2]);
                    data['deceasedNew'] = parseInt(row[1]);
                    data['confirmed'] = parseInt(row[7]);
                    data['confirmedNew'] = parseInt(row[6]);
                    data['deceasedLast7Days'] = parseInt(row[3]);
                    data['deceasedLast7DaysChange'] = parseFloat(row[4]);
                    data['deceasedPerMillion'] = parseFloat(row[5]);
                    data['casesLast7Days'] = parseInt(row[8]);
                    data['casesLast7DaysChange'] = parseFloat(row[9]);
                    data['casesPerMillion'] = parseFloat(row[10]);
                    dataByRegions[regionCode].push(data);
                });
            });
            lastUpdated = body.result.pageContext.rawDataSets.lastUpdate;
        }
    });

    log.info('CRAWLER -- start');
    await crawler.run();
    log.info('CRAWLER -- finish');

    console.log(`Processing and saving data.`);

    const now = new Date();

    const data = {
        dataByStates: dataByStates,
        dataByRegions: dataByRegions,
        statesInfo: statesInfo,
        historyData: "https://api.apify.com/v2/datasets/4wrWtORugf0148gJ6/items?format=json&clean=1",
        sourceUrl: url,
        lastUpdatedAtSource: lastUpdated,
        lastUpdatedAtApify: new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours(), now.getMinutes())).toISOString(),
        readMe: "https://apify.com/davidrychly/covid-who-sprinklr",
    };

    // Save to history
    const latest = await kvStore.getValue(LATEST);
    if (latest && latest.lastUpdatedAtApify) {
        delete latest.lastUpdatedAtApify;
    }
    const actual = Object.assign({}, data);
    delete actual.lastUpdatedAtApify;

    if (JSON.stringify(latest) !== JSON.stringify(actual)) {
        await dataset.pushData(data);
    }

    await kvStore.setValue(LATEST, data);
    await Apify.pushData(data);

    console.log('Done.');
});
