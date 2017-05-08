/*
 * Copyright (c) 2015-2017 Codenvy, S.A.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Codenvy, S.A. - initial API and implementation
 */
'use strict';
import {CheJsonRpcClient} from "./json-rpc-client";

/**
 * This is the JSON RPC clients provider.
 *
 * @author Ann Shumilova
 */
export class JsonRpcClientProvider {
  private clients: Map<string, CheJsonRpcClient

}

