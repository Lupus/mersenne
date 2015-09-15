function me_t() {}

var me;

me_t.prototype.on_ready = function()
{
}

me_t.prototype.set_menu_selected = function(id)
{
	$('#menu .pure-menu-selected').removeClass("pure-menu-selected");
	$('#menu li#' + id).addClass("pure-menu-selected");
}

me_t.prototype.make_loading = function(sel)
{
	var el = $(sel);
	var html = $('#loading_template').render({});
	el.html(html);
}

me_t.prototype.make_error = function(sel, msg)
{
	var el = $(sel);
	var html = $('#error_template').render({
		message: msg,
	});
	el.html(html);
}

me_t.prototype.open_status = function()
{
	$('#main_header').html("Overall status");
	$('#main_header_descr').html("Overall status of mersenne instance");
	me.set_menu_selected('li_overall_status');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		var html = $('#overall_status_template').render(response);
		$('#content').html(html);
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/status",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me_t.prototype.open_config = function()
{
	$('#main_header').html("Configuration");
	$('#main_header_descr').html("Dump of configuration file " +
			"and command line options provided");
	me.set_menu_selected('li_config');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		var html = $('#config_template').render({
			options: response
		});
		$('#content').html(html);
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/config",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me_t.prototype.open_learner = function()
{
	$('#main_header').html("Learner state");
	$('#main_header_descr').html("Dump of learner internal state")
	me.set_menu_selected('li_learner');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		var html = $('#learner_template').render(response);
		$('#content').html(html);
		$('#tabs').tabs();
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/learner",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me_t.prototype.open_proposer = function()
{
	$('#main_header').html("Proposer state");
	$('#main_header_descr').html("Dump of proposer internal state")
	me.set_menu_selected('li_proposer');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		if (response) {
			var html = $('#proposer_template').render(response);
			$('#content').html(html);
		} else {
			me.make_error('#content',
					"This instance is not a leader");
		}
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/proposer",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me_t.prototype.open_acceptor = function()
{
	$('#main_header').html("Acceptor state");
	$('#main_header_descr').html("Dump of acceptor internal state")
	me.set_menu_selected('li_acceptor');
	me.make_loading('#content');
	var complete_cb = function(response) {
		console.log(response);
		var html = $('#acceptor_template').render(response);
		$('#content').html(html);
	};
	var error_cb = function(xhr, ajaxOptions, thrownError) {
		alert(xhr.status);
		me.make_error('#content', "AJAX Request failed with code " +
				xhr.status + ": " + thrownError);
	};
	$.ajax({
		url: "api/acceptor",
		cache: false,
		success: complete_cb,
		error: error_cb,
	});

	return false;
}

me = new me_t();
$(function() { me.on_ready(); });
