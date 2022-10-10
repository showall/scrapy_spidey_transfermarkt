import scrapy
from scrapy.spiders import CrawlSpider,  Rule
from scrapy.linkextractors import LinkExtractor
import re

class MySpiderForPlayers(CrawlSpider):
    user_agent ="Custom"
    name = "player_stats"
    allowed_domains = ["www.transfermarkt.com"]
    #start_urls = ["https://www.transfermarkt.com/wettbewerbe/europa/wettbewerbe?ajax=yw1&page=1"]
    custom_settings = {
        "DOWNLOAD_DELAY" : 4,
        "DOWNLOAD_TIMEOUT" : 30,
        "CONCURRENT_REQUESTS" : 20,
        'FEEDS': {
            'output_folder/%(name)s.csv': {
                'format': 'csv',
                'overwrite': True
            }
        }
    }     
    
    def __init__(self, domain='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        if domain != '':
            self.start_urls = [domain]
        else:
            self.start_urls =  ["https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"]


    rules = (
        Rule(LinkExtractor(allow = r'startseite\/wettbewerb'), follow=False),
        Rule(LinkExtractor(allow = r'startseite\/verein.*saison_id\/\d{3}2'), callback='parse_club_links', follow=False),
     #   Rule(LinkExtractor(allow = r'profil\/spieler'), callback='parse_players', follow=True)
     )

    def parse_club_links(self, response):
        club_links = response.xpath('//*[@class="responsive-table"]//tbody//a//@href')
        for title in club_links.extract() :
            if "/profil/spieler/" in title:            
                yield scrapy.Request(response.urljoin(title), callback=self.parse_players, headers={'User-Agent': 'Custom'})
            else:
                continue


    def parse_players(self, response):
        print(response.url)
        attributes = {}
    
        try:   
            attributes['current_shirt_number'] = response.xpath('//span[@class = "data-header__shirt-number"]/text()'\
                                                        ).extract_first().strip()
        except:
            attributes['current_shirt_number'] = ""
        try : 
            attributes['firstname'] = response.xpath('//span[@class = "data-header__shirt-number"]/../text()'\
                                               ).extract()[1].strip()
        except : 
            attributes['firstname'] = ""
        attributes['lastname'] = response.xpath('//span[@class = "data-header__shirt-number"]/following-sibling::*/text()'\
                                        ).extract_first().strip()
        
        try :
            attributes['int_goals'] = response.xpath('//a[@class = "data-header__content data-header__content--highlight"]/text()'\
            ).extract()[1].strip()
        except:
            attributes['int_goals'] = 0

        try:
            attributes['int_caps'] = response.xpath('//a[@class = "data-header__content data-header__content--highlight"]/text()'\
            ).extract()[0].strip()   
        except:
            attributes['int_caps'] = 0          
#         attributes['main_position'] = response.xpath('//dd[@class = "detail-position__position"]/text()'\
#           ).extract()[0].strip()   
        attributes['main_position'] = response.xpath('//li[@class = "data-header__label" and contains(text(),"Position")]/span/text()'\
          ).extract_first().strip()      
        attributes['other_position'] = [x.strip() for x in response.xpath('//div[@class = "detail-position__title" and contains(text(),"Other position")]/following-sibling::*/text()').extract()]
        try:
            attributes['current_market_value'] = response.xpath('//div[@class = "tm-player-market-value-development__current-value"]/text()'\
              ).extract_first().strip()    
        except:
            attributes['current_market_value'] = "unknown" 
        try:
            attributes['highest_market_value'] = response.xpath('//div[@class = "tm-player-market-value-development__max-value"]/text()'\
              ).extract_first().strip()   
        except:
            attributes['highest_market_value'] = "unknown" 
        try:
            attributes['highest_market_value_on'] = response.xpath('//div[@class = "tm-player-market-value-development__max-value"]/following-sibling::*/text()'\
              ).extract_first().strip()   
        except:
            attributes['highest_market_value_on'] = "unknown" 
       # attributes['full_name'] = response.xpath('//span[contains(text(),"ame")]/following-sibling::*/text()').get().strip()
        attributes['date_of_birth'] = response.xpath('//span[contains(text(),"Date of birth")]/following-sibling::*/a/text()').get().strip()
        attributes['place_of_birth'] = response.xpath('//span[contains(text(),"Place of birth")]/following-sibling::*/span/text()').get().strip()
        attributes['country_of_birth'] =  response.xpath('//span[contains(text(),"Place of birth")]/following-sibling::*/span/img/@alt').get().strip()
        attributes['height'] = response.xpath('//span[contains(text(),"Height")]/following-sibling::*/text()').get().strip().replace("\xa0m","").replace(",","")            
        attributes['citizenship'] = response.xpath('//span[contains(text(),"Citizenship")]/following-sibling::span/img[@class="flaggenrahmen"]/@alt').extract()
        attributes['dominant_foot'] = response.xpath('//span[contains(text(),"Foot")]/following-sibling::*/text()').get().strip()            
        attributes['current_club'] =  response.xpath('//span[contains(text(),"Current club")]/following-sibling::*/a/img/@alt').get().strip()           
        attributes['joined_on'] =  response.xpath('//span[contains(text(),"Joined")]/span/text()').get().strip()             
        attributes['contract_expiring'] =  response.xpath('//span[contains(text(),"Contract expires")]/following-sibling::*/text()').get().strip()              
        try:
            attributes['last_extension'] =  response.xpath('//span[contains(text(),"Date of last contract extension:")]/following-sibling::*/text()').get().strip()              
        except:
            attributes['last_extension'] =  "unknown"  
        dictionary = {}
        transfer_hist_list = []
        dictionary["season"] = [x.strip() for x in response.xpath('//div[@class = "tm-player-transfer-history-grid"]/div[@class = "tm-player-transfer-history-grid__season"]/text()').extract()]
        dictionary["transfer_date"] = [x.strip() for x in response.xpath('//div[@class = "tm-player-transfer-history-grid"]/div[@class = "tm-player-transfer-history-grid__date"]/text()').extract()]
        dictionary["leaving_club"] = [x.strip() for x in response.xpath('//div[@class = "tm-player-transfer-history-grid"]/div[@class = "tm-player-transfer-history-grid__old-club"]/a/img/@alt').extract()]
        dictionary["joining_club"] = [x.strip() for x in response.xpath('//div[@class = "tm-player-transfer-history-grid"]/div[@class = "tm-player-transfer-history-grid__new-club"]/a/img/@alt').extract()]
        dictionary["market_value"] = [x.strip() for x in response.xpath('//div[@class = "tm-player-transfer-history-grid"]/div[@class = "tm-player-transfer-history-grid__market-value"]/text()').extract()]
        dictionary["fee"] = [x.strip() for x in response.xpath('//div[@class = "tm-player-transfer-history-grid"]/div[@class = "tm-player-transfer-history-grid__fee"]/text()').extract()]
        transfer_hist_list.append(dictionary)
        attributes['transfer_history'] = dictionary
        
        text = response.xpath('//script[contains(text(),"skyscraper")]').extract_first()
        text= text.replace("\n","")
        text= text.replace("\'","")
        text= text.replace("x20"," ")
        text= text.replace("\\","")
        text= text.replace("u20AC","Є")
        result = re.search('data:\[\{(.*)\}\]\}\],legend', text)
        data =result.group(1)
        data = data.split(",")
        dictionary2 = {}
        dictionary2["club"] = []
        dictionary2["market_value"] = []
        dictionary2["age"] = []
        dictionary2["date"] = []
        dictionary2["year"] = []
        for row in data:
            try:
                dictionary2["club"].append(re.match('verein:(.*).*', row).group(1))
            except:
                pass
            try:
                dictionary2["age"].append(re.match('age:(.*).*', row).group(1))
            except:
                pass
            try:        
                dictionary2["date"].append(re.match('datum_mw:(.*).*', row).group(1))
            except:
                pass
            try:        
                dictionary2["market_value"].append(re.match('mw:(.*).*', row).group(1))
            except:
                pass
            try:        
                dictionary2["year"].append(re.match(' (\d{4}).*', row).group(1))
            except:
                pass        
        
        attributes['valuation'] = dictionary2  
        
        yield attributes