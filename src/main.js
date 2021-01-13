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
    ])

    if (notificationEmail) {
        await Apify.addWebhook({
            eventTypes: ['ACTOR.RUN.FAILED', 'ACTOR.RUN.TIMED_OUT'],
            requestUrl: `https://api.apify.com/v2/acts/mnmkng~email-notification-webhook/runs?token=${Apify.getEnv().token}`,
            payloadTemplate: `{"notificationEmail": "${notificationEmail}", "eventType": {{eventType}}, "eventData": {{eventData}}, "resource": {{resource}} }`,
        });
    }

    let dataByStates = {};
    let lastUpdated = 'N/A';

    const crawler = new Apify.BasicCrawler({
        requestList,
        handleRequestFunction: async ({request}) => {
            let response;
            let body;
            let countryGroups;
            response = await requestAsBrowser({
                url: request.url,
                json: true,
            });
            body = response.body;
            countryGroups = body.result.pageContext.rawDataSets.countryGroups;
            let countries = [];
            let countriesObj = [];
            countryGroups.forEach(countryObj => {
                var country = countryName(countryObj.value, 'en');
                var coutryCode = countryObj.value;
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
                var country = countryName(countryCode, 'en');
                var countryRows = countriesObj[countryCode].data.rows;
                dataByStates[country] = [];
                countryRows.forEach(row => {
                    let data = {};
                    data['date'] = new Date(row[0]).toISOString();
                    data['deceased'] = parseInt(row[3]);
                    data['deceasedNew'] = parseInt(row[2]);
                    data['confirmed'] = parseInt(row[8]);
                    data['confirmedNew'] = parseInt(row[7]);

                    dataByStates[country].push(data);
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
