require 'mongo'
require 'rspec'

describe "MongoDB geospatial query" do

  RADIUS_FROM_EARTH_IN_KM = 6371
  
  before(:all) do
    @mongo = Mongo::Connection.new
    @db = @mongo.db("learn_mongodb_geospatial")
    @places = @db.collection("places")
  end

  after(:all) do
    @mongo.drop_database("learn_mongodb_geospatial")
    @mongo.close
  end 

  before(:each) do
    @places.create_index([["lonlat", Mongo::GEO2D]])
  end

  after(:each) do
    @places.drop
  end

  def add_place(lon, lat, name=nil)
    @places.insert(
      lonlat: [lon, lat],
      name: (name ? name : "place at [#{lon}, #{lat}]"),
      desc: "desc sample",
      tags: %w(a b c d)
    )
  end

  def add_places(*places_array)
    places_array.each do |array|
      add_place(*array)
    end
  end

  describe "Exact match" do
    it "can find an exact point" do
      add_place 50, 30
      results = @places.find(lonlat: [50, 30])
      results.count.should == 1
    end
  end 

  describe "Ordered list" do
    it "can return places ordered by distance from a given point" do
      add_places [0, 0], [100, 20], [4, 5]
      results = @places.find(lonlat: {"$near" => [0,0]})
      results.map{|n| n['lonlat']}.should == [[0,0], [4,5], [100,20]]
    end

    it "can limit ordered list with max. distance from a given point" do
      add_places [0, 0], [100, 20], [4, 5]
      max_distance_in_degree = 10
      results = @places.find(lonlat: {"$near" => [0,0], "$maxDistance" => max_distance_in_degree})
      results.map{|n| n['lonlat']}.should == [[0,0], [4,5]]
    end
  end 

  describe "Ordered list with distance value" do
    it "can return the distance between a result and the given point" do
      add_places [0, 0], [100, 20], [4, 5]
      results = @db.command(
        geoNear: "places",
        near: [0, 0]
      )
      results["results"].count.should == 3
      results['results'].first['dis'].should == 0.0
    end

    it "can limit the max. distance from the given point" do
      add_places [0, 0], [100, 20], [4, 5]
      results = @db.command(
        geoNear: "places",
        near: [0, 0],
        num: 2
      )
      results["results"].count.should == 2
    end

    it "can return a list of places within a given radius" do
      add_places [0, 0], [10, 0], [1, 0], [9, 0], [0, 9]

      radius_in_degree = 10
      radius_in_km     = radius_in_degree * 111.19 
      center = [0,0]

      # Degree = Radius in KM / (Earth Radius in KM * PI / 180)
      degree = radius_in_km / (RADIUS_FROM_EARTH_IN_KM *  Math::PI / 180)
      results = @places.find(lonlat: {'$near' => [0,0], '$maxDistance' => degree})

      results.count.should == 4
      results.map{|n| n['lonlat']}.should == [[0,0],[1,0],[9,0],[0,9]]
    end
  end 

  describe "Unordered list" do
    it "can return a list of places within a given radius" do
      add_places [0, 0], [10, 0], [1, 0], [9, 0], [0, 9]

      radius_in_degree = 10
      radius_in_km     = radius_in_degree * 111.19 
      center = [0,0]

      # Degree = Radius in KM / (Earth Radius in KM * PI / 180)
      within_degree = radius_in_km / (RADIUS_FROM_EARTH_IN_KM *  Math::PI / 180)
      results = @places.find(lonlat: {"$within" => {"$center" => [center, within_degree]}})

      results.count.should == 4
      results.map{|n| n['lonlat']}.should_not == [[0,0],[1,0],[9,0],[0,9]]
    end
  end

  describe "Spherical Model" do
    it "can return an unordered list of locations" do
      add_places [0, 0], [10, 0], [1, 0], [9, 0], [0, 9]

      radius_in_km = 10 * 111.19 
      center = [0,0]
      
      radian = radius_in_km / RADIUS_FROM_EARTH_IN_KM 
      results = @places.find(lonlat: {"$within" => {"$centerSphere" => [center, radian]}})

      results.count.should == 4
      lonlats = results.map{|n| n['lonlat']}
      lonlats.should_not == [[0,0], [1,0], [9,0], [0,9]]
    end

    it "can return an ordered list" do
      add_places [0, 0], [10, 0], [1, 0], [9, 0], [0, 9]

      radius_in_km = 10 * 111.19 
      center = [0,0]
      
      radian = radius_in_km / RADIUS_FROM_EARTH_IN_KM
      results = @places.find(lonlat: {'$nearSphere' => [0,0], '$maxDistance' => radian})

      results.count.should == 4
      lonlats = results.map{|n| n['lonlat']}
      lonlats.should == [[0,0], [1,0], [9,0], [0,9]]
    end
  end 
end
