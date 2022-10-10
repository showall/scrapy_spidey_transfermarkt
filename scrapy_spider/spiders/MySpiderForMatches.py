import scrapy
from scrapy.spiders import CrawlSpider,  Rule
from scrapy.linkextractors import LinkExtractor
import re

class MySpiderForMatches(CrawlSpider):

    user_agent ="Custom"
    name = "match_logs"
    allowed_domains = ["www.transfermarkt.com"]
    custom_settings = {
        "DOWNLOAD_DELAY" : 4,
        "DOWNLOAD_TIMEOUT" : 20,
        "CONCURRENT_REQUESTS" : 20,
        'FEEDS': {
            'output_folder/%(name)s.csv': {
                'format': 'csv',
                'overwrite': True
            }
        }
    }    
    
    def __init__(self, domain='', start=1, end=1, season_id=2022, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if domain != '':
            self.start_urls = [domain]
        else:
            self.start_urls =  ["https://www.transfermarkt.com/manchester-united/spielplan/verein/985/saison_id/2022"]

        self.start_weekday = start
        self.end_weekday = end
        self.season_id = season_id

    rules = (
    #    Rule(LinkExtractor(allow = r'profil\/spieler'), callback='parse_players', follow=True)
         Rule(LinkExtractor(allow = r'((premier-league)|(laliga)|(serie-a)|(bundesliga)|(ligue-1)|(liga-portugal)|(super-lig)|(eredivisie)|(jupiler-pro-league))\/startseite\/wettbewerb'), follow=True),
         Rule(LinkExtractor(allow = r'startseite\/verein'), follow=True),
         Rule(LinkExtractor(allow = r'spielplan\/verein.*saison_id\/\d{3}2'), callback='parse_club_links_2', follow=False),     
    #     Rule(LinkExtractor(allow = r'gesamtspielplan\/wettbewerb.*saison_id\/\d{3}2'), callback='parse_club_links_1', follow=False),     if go by Fixture page
    )

    def parse_club_links_1(self,response):
      #  print(response)    
        link = response.url 
        try :
            link =  re.sub(r"\/saison_id\/\d{4}","", link)
        except :
            pass

        url_link = link + f"?saison_id={self.season_id}&spieltagVon={self.start_weekday}&spieltagBis={self.end_weekday}"
       #print (url_link)


        yield scrapy.Request(url=url_link, callback=self.parse_club_links_2, headers={'User-Agent': 'Custom'}, dont_filter=True)


    def parse_club_links_2(self,response):
        extracted_scoreline_from_link = response.xpath("//a[contains(@href, 'index/spielbericht')]//text()").extract()

        for i in range(len(extracted_scoreline_from_link)) :
            if re.search(r"\d+:\d+", extracted_scoreline_from_link[i].strip()) != None:
                response_1 = response.xpath("//a[contains(@href, 'index/spielbericht')]//@href").extract()[i]  
            yield scrapy.Request(url=response.urljoin(response_1), callback=self.parse_club_links_3, headers={'User-Agent': 'Custom'}, dont_filter=True)


    def parse_club_links_3(self,response):
      #  print (response)
        attributes = {}
        attributes["match_id"] = response.url.split("/")[-1]
        attributes["home_tag"] = response.xpath('//a[@class="sb-vereinslink"]/@href').getall()[0].strip().split("/")[1]          
        attributes["away_tag"] = response.xpath('//a[@class="sb-vereinslink"]/@href').getall()[1].strip().split("/")[1]
        matchsheet_link =  f'https://www.transfermarkt.com/{attributes["home_tag"]}_{attributes["away_tag"]}/index/spielbericht/{attributes["match_id"]}'
        yield scrapy.Request(url=matchsheet_link, callback=self.parse_match_sheet, headers={'User-Agent': 'Custom'}, 
        meta = {"attributes" : attributes}, dont_filter=True)



    def parse_match_sheet(self,response):
        
        attributes = response.meta["attributes"]
       # attributes["trfmarkt_game_id"] =  response.url.split("/")[-1]
        attributes["home_team"] = response.xpath('//a[@class="sb-vereinslink"]/text()').getall()[0].strip() 
        attributes["away_team"] = response.xpath('//a[@class="sb-vereinslink"]/text()').getall()[1].strip()     

        try:
            row = response.xpath('//p[@data-type="link"]/text()').getall()
            attributes["home_team_position"] = row[0].replace("Position:","").strip()
            attributes["away_team_position"] = row[1].replace("Position:","").strip()        
        except:
            attributes["home_team_position"] = "nil"
            attributes["away_team_position"] = "nil"      

        try:
            row = response.xpath('//div[@class="sb-endstand"]/text()').get().split(":")
            attributes["home_team_score"] =  row[0].strip()
            attributes["away_team_score"] =  row[1].strip()    
        except:
            attributes["home_team_score"] = "nil"
            attributes["away_team_score"]= "nil"     
        
        try:
            attributes["season"] = re.search("(\d{4})",response.xpath('//div[@class="spielername-profil"]//a/@href').get()).group(0)
            attributes["competition"] =  response.xpath('//div[@class="spielername-profil"]//a/text()').get()

        except:
            attributes["season"] = "nil"
            attributes["competition"] = "nil"

        try:
            string = str([x.strip() for x in response.xpath('//p[contains(@class,"sb-datum hide-for-small")]//text()').getall()[0:2]])
            attributes["event"] =  re.sub(r"(:)|(\])|(\[)|(\|)|(,)|(')|([A-Z][a-z]{2}, \d+\/\d+\/\d+)","",string).strip()
        except:
            attributes["event"] = "nil"
        try:
            attributes["date"] =  response.xpath('//p//a[contains(@href,"aktuell/waspassiertheute")]/text()').getall()[0].strip()
 
        except:
            attributes["date"] =  "nil"
        try:
            attributes["time"] =  re.search(r"\d+:\d+ .M",response.xpath('//p//a[contains(@href,"aktuell/waspassiertheute")]/../text()').getall()[2]).group(0)
        except:
            try:
                attributes["time"] =  re.search(r"\d+:\d+ .M",response.xpath('//p//a[contains(@href,"aktuell/waspassiertheute")]/../text()').getall()[1]).group(0)
            except:
                attributes["time"] =  "nil"        
        try:            
            attributes["referee"] =  response.xpath('//p[@class="sb-zusatzinfos"]//a/text()').getall()[1].strip()                
        except:
            attributes["referee"] =  "nil"
        try:
            attributes["away_ht_team_score"] =  response.xpath('//div[@class="sb-halbzeit"]/span/text()').get().replace(":", "").strip()
            attributes["home_ht_team_score"] =  response.xpath('//div[@class="sb-halbzeit"]/span/../text()').getall()[1].replace(")","")

        except:
            attributes["away_ht_team_score"] =   "nil"
            attributes["home_ht_team_score"] =  "nil"

        try :      
            attributes["home_formation"] = response.xpath('//*[@class="large-7 aufstellung-vereinsseite columns small-12 unterueberschrift aufstellung-unterueberschrift"]/text()').getall()[0].replace("Starting Line-up:","#").strip()
            attributes["away_formation"] = response.xpath('//*[@class="large-7 aufstellung-vereinsseite columns small-12 unterueberschrift aufstellung-unterueberschrift"]/text()').getall()[1].replace("Starting Line-up:","#").strip()
        except:
            attributes["home_formation"] =   "nil" 
            attributes["away_formation"] =   "nil"        

        try :
            manager = response.xpath('//div[contains(text(),"Manager")]/parent::*/following-sibling::*/a/text()').getall()
            attributes["home_manager"] = manager[0]
            attributes["away_manager"] = manager[1]
        except:    
            attributes["home_manager"] = "nil"
            attributes["away_manager"] = "nil"       

        try :
            attributes["attendance"] = response.xpath('//p[@class="sb-zusatzinfos"]//strong/text()').get().replace("Attendance:", "").strip().replace(".","")
        except :
            attributes["attendance"] = "nil"   

        line_up = f'https://www.transfermarkt.com/{attributes["home_tag"]}_{attributes["away_tag"]}/aufstellung/spielbericht/{attributes["match_id"]}' 
        yield scrapy.Request(url=line_up, callback=self.parse_lineup, headers={'User-Agent': 'Custom'}, meta= {"attributes" : attributes}, dont_filter=True)

    def parse_lineup(self,response):

        attributes =  response.meta["attributes"]
        try:
            attributes["home_team_foreigners%"] =re.search(r"\d+.\d+%*", response.xpath('//div[@class="table-footer"]//td[contains(text(),"Foreigners")]/text()').getall()[0]).group(0)
            attributes["away_team_foreigners%"] = re.search(r"\d+.\d+%*", response.xpath('//div[@class="table-footer"]//td[contains(text(),"Foreigners")]/text()').getall()[1]).group(0)
            attributes["home_team_avg_age"] = re.search(r"(\d{2}.\d+)", response.xpath('//div[@class="table-footer"]//td[contains(text(),"age")]/text()').getall()[0]).group(0)
            attributes["away_team_avg_age"] = re.search(r"(\d{2}.\d+)", response.xpath('//div[@class="table-footer"]//td[contains(text(),"age")]/text()').getall()[1]).group(0)
  
        except:
            pass

        try:
            attributes["home_team_PV"] =re.search(r"€(\d+.\d+)m", response.xpath('//div[@class="table-footer"]//td[contains(text(),"value")]/text()').getall()[0]).group(0)
        except:
            try:
                attributes["home_team_PV"] =re.search(r"€(\d+.\d+)Th", response.xpath('//div[@class="table-footer"]//td[contains(text(),"value")]/text()').getall()[0]).group(0)
            except:
                    attributes["home_team_PV"] = ""

        try:
            attributes["away_team_PV"] =re.search(r"€(\d+.\d+)m", response.xpath('//div[@class="table-footer"]//td[contains(text(),"value")]/text()').getall()[1]).group(0)
        except:
            try:
                attributes["away_team_PV"] =re.search(r"€(\d+.\d+)Th", response.xpath('//div[@class="table-footer"]//td[contains(text(),"value")]/text()').getall()[1]).group(0)
            except:
                    attributes["away_team_PV"] = ""
                    
        try:
            attributes["home_team_MV"] =re.search(r"€(\d+.\d+)m", response.xpath('//div[@class="table-footer"]//td[contains(text(),"MV")]/text()').getall()[0]).group(0)
        except:
            try:
                attributes["home_team_MV"] =re.search(r"€(\d+.\d+)Th", response.xpath('//div[@class="table-footer"]//td[contains(text(),"MV")]/text()').getall()[0]).group(0)
            except:
                    attributes["home_team_MV"] = ""
                    
        try:
            attributes["away_team_MV"] =re.search(r"€(\d+.\d+)m", response.xpath('//div[@class="table-footer"]//td[contains(text(),"MV")]/text()').getall()[1]).group(0)
        except:
            try:
                attributes["away_team_MV"] =re.search(r"€(\d+.\d+)Th", response.xpath('//div[@class="table-footer"]//td[contains(text(),"MV")]/text()').getall()[1]).group(0)
            except:
                    attributes["away_team_MV"] = ""


        statistics = f'https://www.transfermarkt.com/{attributes["home_tag"]}_{attributes["away_tag"]}/statistik/spielbericht/{attributes["match_id"]}'
       # statistics = f"https://www.transfermarkt.com/{home_tag}_{away_tag}/statistik/spielbericht/{match_id}"  
        yield scrapy.Request(url=statistics, callback=self.parse_match_stats, headers={'User-Agent': 'Custom'}, meta= {"attributes" : attributes} , dont_filter=True) 
        #yield attributes
       # yield scrapy.Request(response.url, callback=self.parse_printed, headers={'User-Agent': 'Custom'}, meta= {"attributes" : attributes} , dont_filter=True)
        print("ok2")        
    
    def parse_match_stats(self,response):
        #### THE POSSESSION% STILL A PROBLEM
        attributes = response.meta["attributes"]
        try:
            attributes["available_capacity"] =  response.xpath('//th[contains(text(),"Available Capacity:")]/following-sibling::*/text()').get().replace(".","")
    
        except:
            pass
        try:   
            attributes["home_total_shots"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[0]
            attributes["away_total_shots"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[1]

            attributes["home_shots_off_target"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[2]
            attributes["away_shots_off_target"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[3]

            attributes["home_shots_saved"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[4]
            attributes["away_shots_saved"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[5]

            attributes["home_corners"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[6]
            attributes["away_corners"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[7]

            attributes["home_freekicks"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[8]
            attributes["away_freekicks"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[9]

            attributes["home_fouls"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[10]
            attributes["away_fouls"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[11]

            attributes["home_offsides"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[12]
            attributes["away_offsides"] = response.xpath('//div[@class="sb-statistik-zahl"]/text()').getall()[13]            
            
        except:
            pass
        print("Parsing........")
        yield attributes
    
    def parse_printed(self,response):       
        print("Parsing........")
        attributes =  response.meta["attributes"]
        yield attributes