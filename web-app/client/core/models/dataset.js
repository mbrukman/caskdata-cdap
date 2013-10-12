/*
 * Dataset Model
 */

define(['core/models/element'], function (Element) {

	var Model = Element.extend({
		type: 'Dataset',
		plural: 'Datasets',
		href: function () {
			return '#/datasets/' + this.get('id');
		}.property(),
		init: function() {

			this._super();
			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());
			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

			this.trackMetric('/reactor/datasets/{id}/store.bytes', 'aggregates', 'storage');

		},

		interpolate: function (path) {

			return path.replace(/\{id\}/, this.get('id'));

		}

	});

	Model.reopenClass({
		type: 'Dataset',
		kind: 'Model',
		find: function (dataset_id, http) {
			var promise = Ember.Deferred.create();

			http.rest('datasets', dataset_id, {cache: true}, function (model, error) {

				model = C.Dataset.create(model);
				promise.resolve(model);

			});

			return promise;
		}
	});

	return Model;

});