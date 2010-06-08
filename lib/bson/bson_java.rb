# encoding: UTF-8
# A thin wrapper for the CBson class
module BSON
  module JMongo
    import com.mongodb.BasicDBList
    import com.mongodb.BasicDBObject
    import com.mongodb.Bytes
    import org.bson.BSONDecoder
    import org.bson.BSONEncoder

    import org.bson.BSONCallback
    import org.bson.types.BasicBSONList
    import org.bson.BasicBSONObject
    import org.bson.types.Binary
    import org.bson.types.ObjectId
    import org.bson.types.Symbol
    import org.bson.types.CodeWScope
  end
end

module BSON
  class BSON_JAVA
    #ENC = JMongo::BSONEncoder.new
    BSON::ObjectID

    ENC = Java::OrgJbson::RubyBSONEncoder.new(JRuby.runtime)
    DEC = JMongo::BSONDecoder.new


    def self.deserialize(buf)
      if buf.is_a? String
        buf = ByteBuffer.new(buf.unpack("C*")) if buf
      end
      callback = Java::OrgJbson::RubyBSONCallback.new(JRuby.runtime)
      DEC.decode(buf.to_a.to_java(Java::byte), callback)
      callback.get
    end

    def deserialize(buf)
      callback = Java::OrgJbson::RubyBSONCallback.new(JRuby.runtime)
      DEC.decode(buf.to_a.to_java(Java::byte), callback)
      callback.get
    end

    def self.serialize(obj, check=false, move=false)
      ENC.encode(obj)
    end

    def to_dbobject obj
      case obj
      when Array
        array_to_dblist obj
      when Hash
        hash_to_dbobject obj
      #when BSON::Binary
      #  JMongo::Binary.new(obj.subtype, obj.to_a)
      when BSON::ObjectID
        JMongo::ObjectId.new(obj.to_s)
      when Regexp
        str     = obj.source
        options = obj.options
        options_flag = 0
        options_flag |= JavaPattern::CASE_INSENSITIVE if ((options & Regexp::IGNORECASE) != 0)
        options_flag |= JavaPattern::MULTILINE  if ((options & Regexp::MULTILINE) != 0)
        #options_flag |= JavaPattern::EXTENDED if ((options & Regexp::EXTENDED) != 0)
        Java::JavaUtilRegex::Pattern.compile(str, options_flag)
      when Symbol
        JMongo::Symbol.new(obj)
      when BSON::Binary
        obj.put_int(obj.size, 0)
        b = JMongo::Binary.new(obj.subtype, obj.to_a.to_java(Java::byte))
        obj.to_a.to_java(Java::byte)
      when BSON::DBRef


      else
        # primitive value, no conversion necessary
        #puts "Un-handled class type [#{obj.class}]"
        obj
      end
    end

    def from_java_object(obj)
      case obj
       when JMongo::BasicBSONList
        obj
       when JMongo::BasicBSONObject
        hsh = {}
        obj.toMap.keySet.each do |key|
          value    = obj.get(key)
          hsh[key] = self.from_java_object(value)
        end
        hsh
       when JMongo::ObjectId
         BSON::ObjectID.from_string(obj.toStringMongod())
       else
         obj
       end
    end

    private

    def hash_to_dbobject doc
      obj = JMongo::BasicDBObject.new

      doc.each_pair do |key, value|
        obj.append(key, to_dbobject(value))
      end

      obj
    end

    def array_to_dblist ary
      list = JMongo::BasicDBList.new

      ary.each_with_index do |element, index|
        list.put(index, to_dbobject(element))
      end

      list
    end
  end
end
