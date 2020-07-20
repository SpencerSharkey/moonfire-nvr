// vim: set et sw=2 ts=2:
//
// This file is part of Moonfire NVR, a security camera network video recorder.
// Copyright (C) 2018 The Moonfire NVR Authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// In addition, as a special exception, the copyright holders give
// permission to link the code of portions of this program with the
// OpenSSL library under certain conditions as described in each
// individual source file, and distribute linked combinations including
// the two.
//
// You must obey the GNU General Public License in all respects for all
// of the code used other than OpenSSL. If you modify file(s) with this
// exception, you may extend this exception to your version of the
// file(s), but you are not obligated to do so. If you do not wish to do
// so, delete this exception statement from your version. If you delete
// this exception statement from all source files in the program, then
// also delete it here.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

import $ from 'jquery';

import 'jquery-ui/themes/base/button.css';
import 'jquery-ui/themes/base/core.css';
import 'jquery-ui/themes/base/dialog.css';
import 'jquery-ui/themes/base/theme.css';
// This not needed for pure dialog, but we want it resizable
import 'jquery-ui/themes/base/resizable.css';

// Get dialog ui widget
import 'jquery-ui/ui/widgets/dialog';
import { event } from 'jquery';
import MoonfireAPI from '../MoonfireAPI';

const api = new MoonfireAPI();

/**
 * Class to implement a simple jQuery dialog based video player.
 */
export default class LiveDialogView {
  /**
   * Construct the player.
   *
   * This does not attach the player to the DOM anywhere! In fact, construction
   * of the necessary video element is delayed until an attach is requested.
   * Since the close of the video removes all traces of it in the DOM, this
   * approach allows repeated use by calling attach again!
   */
  constructor() {
    this.ws = null;
    this.mediaSource = null;
    this.mediaBufferPromise = null;
  }


  /**
   * Attach the player to the specified DOM element.
   *
   * @param {Node} domElement DOM element to attach to
   * @return {LiveDialogView} Returns "this" for chaining.
   */
  attach(domElement) {
    this.videoElement_ = $('<video autoplay="true" />');
    this.dialogElement_ = $('<div class="playdialog" />').append(
        this.videoElement_
    );
    $(domElement).append(this.dialogElement_);
    return this;
  }

  /**
   * Show the player, and start playing the given url.
   *
   * @param  {String} title Title of the video player
   * @param  {Number} width Width of the player
   * @param  {String} url   URL for source media
   * @return {LiveDialogView}       Returns "this" for chaining.
   */
  play(title, width, url) {
    const videoDomElement = this.videoElement_[0];

    this.dialogElement_.dialog({
      title: title,
      width: width,
      close: () => {
        videoDomElement.pause();
        videoDomElement.src = ''; // Remove current source to stop loading
        this.videoElement_ = null;
        this.dialogElement_.remove();
        this.dialogElement_ = null;
        if (this.ws) {
          this.ws.close();
        }
      },
    });

    this.mediaSource = new MediaSource();
    videoDomElement.src = URL.createObjectURL(this.mediaSource);
    this.mediaSource.addEventListener('sourceopen', (e) => {
      // Now that dialog is up, set the src so video starts
      console.log('Live url: ' + url);
      this.ws = new WebSocket(url);

      this.ws.addEventListener('open', () => {
        console.log('live stream opened');
      });

      this.ws.addEventListener('message', async (event) => {
        let data = new Uint8Array(await event.data.arrayBuffer());
        const {headers, body} = this.parseStreamData(data);
        const buffer = await this.getMediaBuffer(headers);
        buffer.appendBuffer(body);
      });
    })    

    // On narrow displays (as defined by index.css), play videos in
    // full-screen mode. When the user exits full-screen mode, close the
    // dialog.
    const narrowWindow = $('#nav').css('float') == 'none';
    if (narrowWindow) {
      console.log('Narrow window; starting video in full-screen mode.');
      videoDomElement.requestFullscreen();
      videoDomElement.addEventListener('fullscreenchange', () => {
        if (document.fullscreenElement !== videoDomElement) {
          console.log('Closing video because user exited full-screen mode.');
          this.dialogElement_.dialog('close');
        }
      });
    }
    return this;
  }

    /**
   * @param  {Uint8Array} data data
   * @return {*} headers and data
   */
  parseStreamData(data) {
    const headers = new Headers();
    let pos = 0;
    while (true) {
      const cr = data.indexOf('\r'.charCodeAt(0), pos);
      if (cr == -1 || data.length == cr + 1 || data[cr + 1] != '\n'.charCodeAt(0)) {
        throw new Error('error parsing headers');
      }
      const line = new TextDecoder('ascii').decode(data.slice(pos, cr));
      pos = cr + 2;
      if (line.length == 0) {
        break;
      }
      const colon = line.indexOf(':');
      if (colon == -1 || line.length == colon + 1 || line[colon + 1] != ' ') {
        throw new Error('error parsing headers');
        return;
      }
      const name = line.substring(0, colon);
      const value = line.substring(colon + 2);
      console.log('header', name, value);
      headers.append(name, value);
    }

    return {
      headers,
      body: data.slice(pos)
    }
  }

  getMediaBuffer(headers) {
    if (this.mediaBufferPromise) {
      return this.mediaBufferPromise;
    }

    return this.mediaBufferPromise = new Promise(async (resolve) => {
      // mediaSource.addEventListener('sourceopen', () => {
      const contentType = headers.get('Content-Type');
      if (!MediaSource.isTypeSupported(contentType)) { 
        console.error('Big error. Codec not supported:' + contentType);
        window.alert('unsupported codec: ' + contentType);
      }
      const buffer = this.mediaSource.addSourceBuffer(contentType);
      buffer.mode = 'sequence';
      
      const initSegmentId = headers.get('X-Video-Sample-Entry-Sha1');
      const req = await fetch(api.initUrl(initSegmentId));
      const initData = await req.arrayBuffer();
      buffer.appendBuffer(initData);
      setTimeout(() => {
        resolve(buffer);
      }, 1); // hack - have to wait or else the buffer isn't ready ?
    });
  }

}
