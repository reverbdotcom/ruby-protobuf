require 'protobuf/field/field_array'
require 'protobuf/logging'
require 'protobuf/wire_type'

module Protobuf
  module Field
    class BaseField
      include ::Protobuf::Logging

      ##
      # Constants
      #

      PACKED_TYPES = [
        ::Protobuf::WireType::VARINT,
        ::Protobuf::WireType::FIXED32,
        ::Protobuf::WireType::FIXED64,
      ].freeze

      ##
      # Attributes
      #

      attr_reader :message_class, :name, :options, :rule, :tag, :type_class

      ##
      # Class Methods
      #

      def self.default
        nil
      end

      ##
      # Constructor
      #

      def initialize(message_class, rule, type_class, name, tag, options)
        @message_class = message_class
        @name          = name
        @rule          = rule
        @tag           = tag
        @type_class    = type_class
        @options       = options

        validate_packed_field if packed?
        define_accessor
      end

      ##
      # Public Instance Methods
      #

      def acceptable?(_value)
        true
      end

      def coerce!(value)
        value
      end

      def decode(_bytes)
        fail NotImplementedError, "#{self.class.name}##{__method__}"
      end

      def default
        options[:default]
      end

      def default_value
        @default_value ||= case
                           when repeated? then ::Protobuf::Field::FieldArray.new(self).freeze
                           when required? then nil
                           when optional? then typed_default_value
                           end
      end

      def deprecated?
        options.key?(:deprecated)
      end

      def encode(_value)
        fail NotImplementedError, "#{self.class.name}##{__method__}"
      end

      def extension?
        options.key?(:extension)
      end

      def enum?
        false
      end

      def getter
        name
      end

      def message?
        false
      end

      def optional?
        rule == :optional
      end

      def packed?
        repeated? && options.key?(:packed)
      end

      def repeated?
        rule == :repeated
      end

      def repeated_message?
        repeated? && message?
      end

      def required?
        rule == :required
      end

      # FIXME: need to cleanup (rename) this warthog of a method.
      def set(message_instance, bytes)
        if packed?
          array = message_instance.__send__(getter)
          method = \
            case wire_type
            when ::Protobuf::WireType::FIXED32 then :read_fixed32
            when ::Protobuf::WireType::FIXED64 then :read_fixed64
            when ::Protobuf::WireType::VARINT  then :read_varint
            end
          stream = StringIO.new(bytes)

          until stream.eof?
            array << decode(::Protobuf::Decoder.__send__(method, stream))
          end
        else
          value = decode(bytes)
          if repeated?
            message_instance.__send__(getter) << value
          else
            message_instance.__send__(setter, value)
          end
        end
      end

      def setter
        @setter ||= "#{name}="
      end

      # FIXME: add packed, deprecated, extension options to to_s output
      def to_s
        "#{rule} #{type_class} #{name} = #{tag} #{default ? "[default=#{default.inspect}]" : ''}"
      end

      ::Protobuf.deprecator.define_deprecated_methods(self, :type => :type_class)

      def wire_type
        ::Protobuf::WireType::VARINT
      end

      private

      ##
      # Private Instance Methods
      #

      def define_accessor
        define_field_accessor

        if repeated?
          define_array_getter
          define_array_setter
        else
          define_getter
          define_setter
        end
      end

      ##
      # Example
      #
      # def records
      #   field = self.class._field_records
      #   @values[field.name] ||= ::Protobuf::Field::FieldArray.new(field)
      # end
      #
      def define_array_getter
        message_class.class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{getter}
            field = self.class._field_#{getter}
            @values[field.name] ||= ::Protobuf::Field::FieldArray.new(field)
          end
        RUBY

        ::Protobuf.field_deprecator.deprecate_method(message_class, getter) if deprecated?
      end

      ##
      # Example
      #
      #
      # def records=(values)
      #   field = self.class._field_records
      #   if value.is_a?(Array)
      #     value = value.dup
      #     value.compact!
      #   else
      #     fail TypeError, <<-TYPE_ERROR
      #       Expected repeated value of type '#{field.type_class}'
      #       Got '#{value.class}' for repeated protobuf field #{field.name}
      #     TYPE_ERROR
      #   end
      #
      #   if value.nil? || (value.respond_to?(:empty?) && value.empty?)
      #     @values.delete(field.name)
      #   else
      #     @values[field.name] ||= ::Protobuf::Field::FieldArray.new(field)
      #     @values[field.name].replace(value)
      #   end
      # end
      #
      def define_array_setter
        message_class.class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{setter}(value)
            field = self.class._field_#{getter}
            @values[field.name] ||= ::Protobuf::Field::FieldArray.new(field)

            if value.is_a?(Array)
              value = value.dup
              value.compact!
            else
              fail TypeError, <<-TYPE_ERROR
                Expected repeated value of type '\#{field.type_class}'
                Got '\#{value.class}' for repeated protobuf field \#{field.name}
              TYPE_ERROR
            end

            if value.nil? || (value.respond_to?(:empty?) && value.empty?)
              @values.delete(field.name)
            else
              @values[field.name] ||= ::Protobuf::Field::FieldArray.new(field)
              @values[field.name].replace(value)
            end
          end
        RUBY

        ::Protobuf.field_deprecator.deprecate_method(message_class, setter) if deprecated?
      end

      ##
      # Example
      #
      # class << self
      #   attr_accessor :_field_created_at
      # end
      #
      def define_field_accessor
        message_class.class_eval <<-RUBY, __FILE__, __LINE__ + 1
          class << self
            attr_accessor :_field_#{getter}
          end
        RUBY

        setter_accessor_name = "_field_#{setter}"
        message_class.send(setter_accessor_name, self)
      end

      ##
      # Example
      #
      # def created_at
      #   field = self.class._field_created_at
      #   @values.fetch(field.name, field.default_value)
      # end
      #
      def define_getter
        message_class.class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{getter}
            field = self.class._field_#{getter}
            @values.fetch(field.name, field.default_value)
          end
        RUBY

        ::Protobuf.field_deprecator.deprecate_method(message_class, getter) if deprecated?
      end

      ##
      # Example
      #
      # def created_at=(value)
      #   field = self.class._field_created_at
      #   if value.nil? || (value.respond_to?(:empty?) && value.empty?)
      #     @valueues.delete(field.name)
      #   elsif field.acceptable?(value)
      #     @valueues[field.name] = field.coerce!(value)
      #   else
      #     fail TypeError, "Unacceptable value #{value} for field #{field.name} of type #{field.type_class}"
      #   end
      # end
      #
      def define_setter
        message_class.class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{setter}(value)
            field = self.class._field_#{getter}
            if value.nil? || (value.respond_to?(:empty?) && value.empty?)
              @values.delete(field.name)
            elsif field.acceptable?(value)
              @values[field.name] = field.coerce!(value)
            else
              fail TypeError, "Unacceptable value \#{value} for field \#{field.name} of type \#{field.type_class}"
            end
          end
        RUBY

        ::Protobuf.field_deprecator.deprecate_method(message_class, setter) if deprecated?
      end

      def typed_default_value
        if default.nil?
          self.class.default
        else
          default
        end
      end

      def validate_packed_field
        if packed? && ! ::Protobuf::Field::BaseField::PACKED_TYPES.include?(wire_type)
          fail "Can't use packed encoding for '#{type_class}' type"
        end
      end

    end
  end
end
